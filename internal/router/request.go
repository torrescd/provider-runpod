// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package router

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"regexp"
	"slices"
	"strings"
)

const (
	openCodeSchemaURI    = "https://json-schema.org/draft/2020-12/schema"
	maxMessages          = 128
	maxTextBytes         = 128 << 10
	maxTools             = 32
	maxToolCalls         = 32
	maxToolDescription   = 16 << 10
	maxSchemaDepth       = 3
	maxSchemaNodes       = 128
	maxSchemaProperties  = 64
	maxToolArgumentBytes = 32 << 10
	maxArgumentDepth     = 6
	maxArgumentNodes     = 128
)

var safeName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

var topLevelChatKeys = map[string]struct{}{
	"model": {}, "messages": {}, "max_tokens": {}, "temperature": {}, "top_p": {},
	"frequency_penalty": {}, "presence_penalty": {}, "stop": {}, "seed": {},
	"stream": {}, "stream_options": {}, "tools": {}, "tool_choice": {},
}

// validateChatPayload implements the exact OpenAI-compatible subset exercised
// by pinned OpenCode. Unknown fields fail closed so vLLM-specific extensions
// cannot bypass output, compute, media-fetch, priority, cache, or template
// boundaries at the worker.
func validateChatPayload(payload map[string]any) error {
	if !onlyKeys(payload, topLevelChatKeys) {
		return errors.New("unknown top-level field")
	}
	if model, ok := payload["model"].(string); !ok || model != LogicalModelID {
		return errors.New("invalid logical model")
	}
	maxTokens, ok := integer(payload["max_tokens"])
	if !ok || maxTokens < 1 || maxTokens > 4096 {
		return errors.New("max_tokens must be between 1 and 4096")
	}
	if !optionalNumberInRange(payload, "temperature", 0, 2) ||
		!optionalNumberInRange(payload, "top_p", 0, 1) ||
		!optionalNumberInRange(payload, "frequency_penalty", -2, 2) ||
		!optionalNumberInRange(payload, "presence_penalty", -2, 2) {
		return errors.New("sampling field is out of bounds")
	}
	if value, present := payload["seed"]; present {
		seed, ok := integer(value)
		if !ok || seed < math.MinInt32 || seed > math.MaxInt32 {
			return errors.New("seed is out of bounds")
		}
	}
	if value, present := payload["stream"]; present {
		if _, ok := value.(bool); !ok {
			return errors.New("stream must be boolean")
		}
	}
	if err := validateStreamOptions(payload); err != nil {
		return err
	}
	stop, hasStop := payload["stop"]
	if err := validateStop(stop, hasStop); err != nil {
		return err
	}

	tools, hasTools := payload["tools"]
	declaredTools, err := validateTools(tools, hasTools)
	if err != nil {
		return err
	}
	callIDs, err := validateMessages(payload["messages"], declaredTools)
	if err != nil {
		return err
	}
	toolChoice, hasToolChoice := payload["tool_choice"]
	if err := validateToolChoice(toolChoice, hasToolChoice, declaredTools); err != nil {
		return err
	}
	_ = callIDs
	return nil
}

func validateMessages(value any, declaredTools map[string]struct{}) (map[string]struct{}, error) {
	messages, ok := value.([]any)
	if !ok || len(messages) == 0 || len(messages) > maxMessages {
		return nil, errors.New("messages must be a bounded non-empty array")
	}
	callIDs := map[string]struct{}{}
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("message must be an object")
		}
		role, ok := message["role"].(string)
		if !ok {
			return nil, errors.New("message role is required")
		}
		switch role {
		case "system":
			if !onlyKeys(message, keys("role", "content", "name")) || !validText(message["content"]) || !validOptionalName(message) {
				return nil, errors.New("invalid system message")
			}
		case "user":
			if !onlyKeys(message, keys("role", "content", "name")) || !validUserContent(message["content"]) || !validOptionalName(message) {
				return nil, errors.New("invalid user message")
			}
		case "assistant":
			if !onlyKeys(message, keys("role", "content", "name", "tool_calls")) || !validOptionalName(message) {
				return nil, errors.New("invalid assistant message shape")
			}
			content, hasContent := message["content"]
			calls, hasCalls := message["tool_calls"]
			if hasContent && content != nil && !validText(content) {
				return nil, errors.New("assistant content must be text or null")
			}
			if (!hasContent || content == nil) && !hasCalls {
				return nil, errors.New("assistant message needs content or tool calls")
			}
			if hasCalls {
				ids, err := validateToolCalls(calls, declaredTools)
				if err != nil {
					return nil, err
				}
				for id := range ids {
					if _, duplicate := callIDs[id]; duplicate {
						return nil, errors.New("duplicate tool call ID")
					}
					callIDs[id] = struct{}{}
				}
			}
		case "tool":
			if !onlyKeys(message, keys("role", "content", "tool_call_id")) || !validText(message["content"]) {
				return nil, errors.New("invalid tool message")
			}
			id, ok := message["tool_call_id"].(string)
			if !ok || !safeName.MatchString(id) {
				return nil, errors.New("invalid tool_call_id")
			}
		default:
			return nil, errors.New("unsupported message role")
		}
	}
	// A tool result may refer only to a tool call present earlier in this body.
	for _, raw := range messages {
		message := raw.(map[string]any)
		if message["role"] == "tool" {
			if _, exists := callIDs[message["tool_call_id"].(string)]; !exists {
				return nil, errors.New("tool message references an undeclared call")
			}
		}
	}
	return callIDs, nil
}

func validUserContent(value any) bool {
	if validText(value) {
		return true
	}
	parts, ok := value.([]any)
	if !ok || len(parts) == 0 || len(parts) > 128 {
		return false
	}
	for _, raw := range parts {
		part, ok := raw.(map[string]any)
		if !ok || !onlyKeys(part, keys("type", "text")) || part["type"] != "text" || !validText(part["text"]) {
			return false
		}
	}
	return true
}

func validateTools(value any, present bool) (map[string]struct{}, error) {
	names := map[string]struct{}{}
	if !present {
		return names, nil
	}
	tools, ok := value.([]any)
	if !ok || len(tools) == 0 || len(tools) > maxTools {
		return nil, errors.New("tools must be a bounded non-empty array")
	}
	// vLLM compiles the complete tool grammar. Bound the aggregate request,
	// rather than granting every tool a separate schema budget.
	budget := maxSchemaNodes
	for _, raw := range tools {
		tool, ok := raw.(map[string]any)
		if !ok || !onlyKeys(tool, keys("type", "function")) || tool["type"] != "function" {
			return nil, errors.New("only exact function tools are accepted")
		}
		function, ok := tool["function"].(map[string]any)
		if !ok || !onlyKeys(function, keys("name", "description", "parameters")) || len(function) != 3 {
			return nil, errors.New("invalid function definition")
		}
		name, ok := function["name"].(string)
		if !ok || !safeName.MatchString(name) {
			return nil, errors.New("invalid function name")
		}
		if _, duplicate := names[name]; duplicate {
			return nil, errors.New("duplicate function name")
		}
		names[name] = struct{}{}
		description, ok := function["description"].(string)
		if !ok || len(description) > maxToolDescription {
			return nil, errors.New("invalid function description")
		}
		parameters, present := function["parameters"]
		if !present {
			return nil, errors.New("function parameters are required")
		}
		if err := validateJSONSchema(parameters, 0, &budget); err != nil {
			return nil, err
		}
	}
	return names, nil
}

func validateToolCalls(value any, declared map[string]struct{}) (map[string]struct{}, error) {
	calls, ok := value.([]any)
	if !ok || len(calls) == 0 || len(calls) > maxToolCalls {
		return nil, errors.New("tool_calls must be a bounded non-empty array")
	}
	ids := map[string]struct{}{}
	for _, raw := range calls {
		call, ok := raw.(map[string]any)
		if !ok || !onlyKeys(call, keys("id", "type", "function")) || call["type"] != "function" {
			return nil, errors.New("invalid tool call")
		}
		id, ok := call["id"].(string)
		if !ok || !safeName.MatchString(id) {
			return nil, errors.New("invalid tool call ID")
		}
		if _, duplicate := ids[id]; duplicate {
			return nil, errors.New("duplicate tool call ID")
		}
		ids[id] = struct{}{}
		function, ok := call["function"].(map[string]any)
		if !ok || !onlyKeys(function, keys("name", "arguments")) {
			return nil, errors.New("invalid called function")
		}
		name, ok := function["name"].(string)
		if !ok || !safeName.MatchString(name) {
			return nil, errors.New("invalid called function name")
		}
		if _, exists := declared[name]; !exists {
			return nil, errors.New("called function was not declared")
		}
		arguments, ok := function["arguments"].(string)
		if !ok || len(arguments) == 0 || len(arguments) > maxToolArgumentBytes {
			return nil, errors.New("invalid function arguments")
		}
		canonical, err := validateToolArguments(arguments)
		if err != nil {
			return nil, err
		}
		function["arguments"] = canonical
	}
	return ids, nil
}

func validateToolChoice(value any, present bool, declared map[string]struct{}) error {
	if !present {
		return nil
	}
	if choice, ok := value.(string); ok {
		if slices.Contains([]string{"auto", "none", "required"}, choice) {
			return nil
		}
		return errors.New("invalid tool_choice")
	}
	choice, ok := value.(map[string]any)
	if !ok || !onlyKeys(choice, keys("type", "function")) || choice["type"] != "function" {
		return errors.New("invalid named tool_choice")
	}
	function, ok := choice["function"].(map[string]any)
	if !ok || !onlyKeys(function, keys("name")) {
		return errors.New("invalid named tool_choice function")
	}
	name, ok := function["name"].(string)
	if !ok {
		return errors.New("invalid named tool_choice function")
	}
	if _, exists := declared[name]; !exists {
		return errors.New("tool_choice references an undeclared function")
	}
	return nil
}

func validateJSONSchema(value any, depth int, budget *int) error {
	if depth > maxSchemaDepth || *budget <= 0 {
		return errors.New("tool JSON Schema exceeds complexity limits")
	}
	*budget = *budget - 1
	schema, ok := value.(map[string]any)
	if !ok {
		return errors.New("tool JSON Schema must be an object")
	}
	// This is the exact keyword inventory emitted by the pinned OpenCode
	// v1.18.23 capture. Expanding it requires a new compatibility capture and
	// review because remote vLLM consumes this schema as executable grammar.
	allowed := keys("type", "description", "default", "properties", "required", "items", "enum", "minimum", "maximum", "exclusiveMinimum")
	if depth == 0 {
		allowed["$schema"] = struct{}{}
	}
	if !onlyKeys(schema, allowed) {
		return errors.New("tool JSON Schema uses an unsupported keyword")
	}
	rawDialect, hasDialect := schema["$schema"]
	if depth == 0 && !hasDialect {
		return errors.New("root tool JSON Schema dialect is required")
	}
	if raw := rawDialect; hasDialect {
		uri, ok := raw.(string)
		if depth != 0 || !ok || uri != openCodeSchemaURI {
			return errors.New("tool JSON Schema dialect is not the pinned OpenCode dialect")
		}
		// Pinned OpenCode v1.18.23 adds this dialect declaration to every
		// function input schema. It is compatibility metadata rather than a
		// worker instruction, so validate it exactly and strip it before the
		// request crosses the trust boundary.
		delete(schema, "$schema")
	}
	typeName, ok := schema["type"].(string)
	if !ok || !slices.Contains([]string{"object", "array", "string", "number", "integer", "boolean"}, typeName) {
		return errors.New("every tool JSON Schema node requires a supported type")
	}
	if depth == 0 && typeName != "object" {
		return errors.New("root tool JSON Schema must be an object")
	}
	if description, present := schema["description"]; present {
		text, ok := description.(string)
		if !ok || len(text) > maxToolDescription {
			return errors.New("invalid JSON Schema description")
		}
	}
	if raw, present := schema["default"]; present {
		if !schemaPrimitiveMatches(typeName, raw) {
			return errors.New("JSON Schema default must match its primitive type")
		}
		// OpenCode uses primitive defaults as local UI/tool annotations. Strip
		// them so they cannot affect remote vLLM schema/tool behavior.
		delete(schema, "default")
	}
	properties := map[string]any{}
	if raw, present := schema["properties"]; present {
		if typeName != "object" {
			return errors.New("JSON Schema properties require object type")
		}
		var ok bool
		properties, ok = raw.(map[string]any)
		if !ok || len(properties) > maxSchemaProperties {
			return errors.New("invalid JSON Schema properties")
		}
		for name, child := range properties {
			if name == "" || len(name) > 128 {
				return errors.New("invalid JSON Schema property name")
			}
			if err := validateJSONSchema(child, depth+1, budget); err != nil {
				return err
			}
		}
	}
	if raw, present := schema["required"]; present {
		if typeName != "object" {
			return errors.New("JSON Schema required requires object type")
		}
		required, ok := stringArray(raw, maxSchemaProperties, 128)
		if !ok {
			return errors.New("invalid JSON Schema required list")
		}
		seen := map[string]struct{}{}
		for _, name := range required {
			if _, exists := properties[name]; !exists {
				return errors.New("required property is not declared")
			}
			if _, duplicate := seen[name]; duplicate {
				return errors.New("duplicate required property")
			}
			seen[name] = struct{}{}
		}
	}
	if typeName == "object" {
		if len(properties) == 0 {
			return errors.New("object tool JSON Schema requires non-empty properties")
		}
		rawRequired, present := schema["required"]
		required, ok := stringArray(rawRequired, maxSchemaProperties, 128)
		if !present || !ok || len(required) == 0 {
			return errors.New("object tool JSON Schema requires a non-empty required list")
		}
	}
	if raw, present := schema["items"]; present {
		if typeName != "array" {
			return errors.New("JSON Schema items require array type")
		}
		if err := validateJSONSchema(raw, depth+1, budget); err != nil {
			return err
		}
	}
	if typeName == "array" {
		if _, present := schema["items"]; !present {
			return errors.New("array tool JSON Schema requires items")
		}
	}
	if raw, present := schema["enum"]; present {
		if typeName == "object" || typeName == "array" {
			return errors.New("JSON Schema enum is limited to captured primitive types")
		}
		values, ok := raw.([]any)
		if !ok || len(values) == 0 || len(values) > 64 {
			return errors.New("invalid JSON Schema enum")
		}
		for _, value := range values {
			if !schemaPrimitiveMatches(typeName, value) {
				return errors.New("JSON Schema enum value does not match its declared type")
			}
		}
	}
	for _, keyword := range []string{"minimum", "maximum", "exclusiveMinimum"} {
		if raw, present := schema[keyword]; present {
			if typeName != "number" && typeName != "integer" {
				return errors.New("numeric JSON Schema bound requires number or integer type")
			}
			if _, ok := number(raw); !ok {
				return errors.New("invalid numeric JSON Schema bound")
			}
		}
	}
	return nil
}

func validateStreamOptions(payload map[string]any) error {
	raw, present := payload["stream_options"]
	if !present {
		return nil
	}
	options, ok := raw.(map[string]any)
	if !ok || !onlyKeys(options, keys("include_usage")) || len(options) != 1 {
		return errors.New("invalid stream_options")
	}
	if _, ok := options["include_usage"].(bool); !ok || payload["stream"] != true {
		return errors.New("stream_options requires streaming")
	}
	return nil
}

func validateStop(value any, present bool) error {
	if !present {
		return nil
	}
	if text, ok := value.(string); ok {
		if len(text) == 0 || len(text) > 256 {
			return errors.New("invalid stop string")
		}
		return nil
	}
	values, ok := stringArray(value, 4, 256)
	if !ok || len(values) == 0 {
		return errors.New("invalid stop array")
	}
	return nil
}

func validText(value any) bool {
	text, ok := value.(string)
	return ok && len(text) <= maxTextBytes
}

func validOptionalName(value map[string]any) bool {
	raw, present := value["name"]
	if !present {
		return true
	}
	name, ok := raw.(string)
	return ok && safeName.MatchString(name)
}

func optionalNumberInRange(payload map[string]any, key string, minimum, maximum float64) bool {
	raw, present := payload[key]
	if !present {
		return true
	}
	value, ok := number(raw)
	return ok && value >= minimum && value <= maximum
}

func number(value any) (float64, bool) {
	n, ok := value.(float64)
	return n, ok && !math.IsNaN(n) && !math.IsInf(n, 0)
}

func integer(value any) (float64, bool) {
	n, ok := number(value)
	return n, ok && math.Trunc(n) == n
}

func stringArray(value any, maximum, maxBytes int) ([]string, bool) {
	raw, ok := value.([]any)
	if !ok || len(raw) > maximum {
		return nil, false
	}
	values := make([]string, len(raw))
	for i := range raw {
		text, ok := raw[i].(string)
		if !ok || text == "" || len(text) > maxBytes {
			return nil, false
		}
		values[i] = text
	}
	return values, true
}

func jsonPrimitive(value any) bool {
	switch value.(type) {
	case nil, bool, string, float64:
		return true
	default:
		return false
	}
}

func schemaPrimitiveMatches(typeName string, value any) bool {
	switch typeName {
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := number(value)
		return ok
	case "integer":
		_, ok := integer(value)
		return ok
	default:
		return false
	}
}

func validateToolArguments(encoded string) (string, error) {
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.UseNumber()
	budget := maxArgumentNodes
	textBudget := maxToolArgumentBytes
	value, err := decodeStrictJSONValue(decoder, 0, &budget, &textBudget)
	if err != nil {
		return "", errors.New("invalid function arguments JSON")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return "", errors.New("function arguments must be exactly one JSON object")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return "", errors.New("function arguments contain trailing JSON")
	}
	if containsSecretKey(object) {
		return "", errors.New("function arguments contain a secret-labelled field")
	}
	canonical, err := json.Marshal(object)
	if err != nil || len(canonical) > maxToolArgumentBytes || credentialPattern.Match(canonical) {
		return "", errors.New("function arguments contain credential-like material")
	}
	return string(canonical), nil
}

func decodeStrictJSONValue(decoder *json.Decoder, depth int, budget, textBudget *int) (any, error) {
	if depth > maxArgumentDepth || *budget <= 0 {
		return nil, errors.New("JSON argument complexity exceeded")
	}
	*budget--
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			object := map[string]any{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok || key == "" || len(key) > 128 {
					return nil, errors.New("invalid JSON argument key")
				}
				if _, duplicate := object[key]; duplicate {
					return nil, errors.New("duplicate JSON argument key")
				}
				*textBudget -= len(key)
				if *textBudget < 0 {
					return nil, errors.New("JSON argument text budget exceeded")
				}
				child, err := decodeStrictJSONValue(decoder, depth+1, budget, textBudget)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim('}') {
				return nil, errors.New("unterminated JSON argument object")
			}
			return object, nil
		case '[':
			values := make([]any, 0)
			for decoder.More() {
				child, err := decodeStrictJSONValue(decoder, depth+1, budget, textBudget)
				if err != nil {
					return nil, err
				}
				values = append(values, child)
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim(']') {
				return nil, errors.New("unterminated JSON argument array")
			}
			return values, nil
		default:
			return nil, errors.New("unexpected JSON argument delimiter")
		}
	case string:
		*textBudget -= len(value)
		if *textBudget < 0 {
			return nil, errors.New("JSON argument text budget exceeded")
		}
		return value, nil
	case json.Number, bool, nil:
		return value, nil
	default:
		return nil, errors.New("unsupported JSON argument value")
	}
}

func onlyKeys(value map[string]any, allowed map[string]struct{}) bool {
	for key := range value {
		if _, ok := allowed[key]; !ok {
			return false
		}
	}
	return true
}

func keys(values ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}
