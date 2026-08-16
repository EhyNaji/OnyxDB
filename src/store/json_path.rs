//! JSON path parsing and mutation for the supported field/index subset.
// ============================================================
// JSON PATH — field access and array indices only; no wildcards or filters.
// ============================================================

#[derive(Debug, Clone, PartialEq)]
pub(super) enum JsonPathSegment {
    Field(String),
    Index(usize),
}
/// Parses a path such as `$.a.b[2].c` into navigation segments. `$` denotes
/// the document root and produces an empty segment list. Syntactically invalid
/// paths return `None`; data presence is evaluated during navigation.
pub(super) fn parse_json_path(path: &str) -> Option<Vec<JsonPathSegment>> {
    let path = path.trim();
    if path != "$" && !path.starts_with('$') {
        return None; // Every valid path begins with '$'.
    }
    let rest = &path[1..]; // Skip the leading '$'.
    if rest.is_empty() {
        return Some(Vec::new()); // `$` addresses the complete document.
    }
    if !rest.starts_with('.') && !rest.starts_with('[') {
        return None; // `$` must be followed by `.` or `[`.
    }

    let mut segments = Vec::new();
    let chars: Vec<char> = rest.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '.' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
                    i += 1;
                }
                if start == i {
                    return None; // Reject empty or trailing field segments.
                }
                let field: String = chars[start..i].iter().collect();
                segments.push(JsonPathSegment::Field(field));
            }
            '[' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != ']' {
                    i += 1;
                }
                if i >= chars.len() {
                    return None; // Reject an unclosed array index.
                }
                let idx_str: String = chars[start..i].iter().collect();
                let idx: usize = idx_str.parse().ok()?; // Only non-negative indices.
                segments.push(JsonPathSegment::Index(idx));
                i += 1; // Skip the closing ']'.
            }
            _ => return None, // Reject characters outside field or array segments.
        }
    }

    Some(segments)
}
/// Returns the node addressed by `segments`, or `None` on absence/type mismatch.
pub(super) fn get_json_path<'a>(
    root: &'a serde_json::Value,
    segments: &[JsonPathSegment],
) -> Option<&'a serde_json::Value> {
    let mut current = root;
    for seg in segments {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => map.get(f)?,
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => arr.get(*idx)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Sets the addressed value and may create only the final segment. Missing or
/// incompatible intermediate segments cause the operation to fail.
pub(super) fn set_json_path(
    root: &mut serde_json::Value,
    segments: &[JsonPathSegment],
    new_value: serde_json::Value,
) -> bool {
    if segments.is_empty() {
        *root = new_value;
        return true;
    }
    let mut current = root;
    for seg in &segments[..segments.len() - 1] {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => {
                match map.get_mut(f) {
                    Some(v) => v,
                    None => return false, // Intermediate objects are not created.
                }
            }
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
                match arr.get_mut(*idx) {
                    Some(v) => v,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    match (&segments[segments.len() - 1], current) {
        (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => {
            map.insert(f.clone(), new_value);
            true
        }
        (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
            if *idx < arr.len() {
                arr[*idx] = new_value;
                true
            } else if *idx == arr.len() {
                arr.push(new_value); // An index at the array length appends.
                true
            } else {
                false // Do not create sparse arrays.
            }
        }
        _ => false,
    }
}

/// Removes the addressed node and reports whether a value was removed.
pub(super) fn delete_json_path(root: &mut serde_json::Value, segments: &[JsonPathSegment]) -> bool {
    if segments.is_empty() {
        return false; // Root deletion is handled by normal key deletion.
    }
    let mut current = root;
    for seg in &segments[..segments.len() - 1] {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => match map.get_mut(f) {
                Some(v) => v,
                None => return false,
            },
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
                match arr.get_mut(*idx) {
                    Some(v) => v,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    match (&segments[segments.len() - 1], current) {
        (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => map.remove(f).is_some(),
        (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) if *idx < arr.len() => {
            arr.remove(*idx);
            true
        }
        _ => false,
    }
}
/// Returns a mutable reference to the addressed node for in-place operations.
fn get_json_path_mut<'a>(
    root: &'a mut serde_json::Value,
    segments: &[JsonPathSegment],
) -> Option<&'a mut serde_json::Value> {
    let mut current = root;
    for seg in segments {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => map.get_mut(f)?,
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => arr.get_mut(*idx)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Increments an existing JSON number. Missing paths are not created as zero.
pub(super) fn numincrby_json_path(
    root: &mut serde_json::Value,
    segments: &[JsonPathSegment],
    delta: f64,
) -> Result<f64, &'static str> {
    let node = get_json_path_mut(root, segments).ok_or("ERR path JSON not found")?;
    let current = node
        .as_f64()
        .ok_or("WRONGTYPE the value at the path is not a number")?;
    let new_val = current + delta;
    let new_number = if new_val.fract() == 0.0 && new_val.abs() < i64::MAX as f64 {
        serde_json::Number::from(new_val as i64)
    } else {
        serde_json::Number::from_f64(new_val)
            .ok_or("ERR invalid numeric result (NaN or infinity)")?
    };
    *node = serde_json::Value::Number(new_number);
    Ok(new_val)
}

/// Appends to an existing JSON array and rejects missing or incompatible paths.
pub(super) fn arrappend_json_path(
    root: &mut serde_json::Value,
    segments: &[JsonPathSegment],
    new_value: serde_json::Value,
) -> Result<usize, &'static str> {
    let node = get_json_path_mut(root, segments).ok_or("ERR path JSON not found")?;
    match node {
        serde_json::Value::Array(arr) => {
            arr.push(new_value);
            Ok(arr.len())
        }
        _ => Err("WRONGTYPE the value at the path is not an array"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // ============================================================
    // JSON path parsing and navigation.
    // ============================================================

    #[test]
    fn parse_json_root_path() {
        assert_eq!(parse_json_path("$"), Some(vec![]));
    }

    #[test]
    fn parse_single_json_field() {
        assert_eq!(
            parse_json_path("$.name"),
            Some(vec![JsonPathSegment::Field("name".to_string())])
        );
    }

    #[test]
    fn parse_nested_json_fields() {
        assert_eq!(
            parse_json_path("$.address.city"),
            Some(vec![
                JsonPathSegment::Field("address".to_string()),
                JsonPathSegment::Field("city".to_string()),
            ])
        );
    }

    #[test]
    fn parse_json_array_index() {
        assert_eq!(
            parse_json_path("$.tag[0]"),
            Some(vec![
                JsonPathSegment::Field("tag".to_string()),
                JsonPathSegment::Index(0)
            ])
        );
    }

    #[test]
    fn parse_long_mixed_json_path() {
        assert_eq!(
            parse_json_path("$.a[1].b[2]"),
            Some(vec![
                JsonPathSegment::Field("a".to_string()),
                JsonPathSegment::Index(1),
                JsonPathSegment::Field("b".to_string()),
                JsonPathSegment::Index(2),
            ])
        );
    }

    #[test]
    fn json_path_requires_a_root_marker() {
        assert_eq!(parse_json_path("name"), None);
    }

    #[test]
    fn json_path_rejects_an_empty_field_segment() {
        assert_eq!(parse_json_path("$..name"), None);
    }

    #[test]
    fn json_path_rejects_an_unclosed_array_index() {
        assert_eq!(parse_json_path("$.tag[0"), None);
    }

    #[test]
    fn json_path_rejects_a_non_numeric_array_index() {
        assert_eq!(parse_json_path("$.tag[x]"), None);
    }

    #[test]
    fn get_json_path_returns_an_existing_field() {
        let val: serde_json::Value = serde_json::json!({"name": "Marco", "age": 18});
        let path = parse_json_path("$.name").unwrap();
        assert_eq!(
            get_json_path(&val, &path),
            Some(&serde_json::json!("Marco"))
        );
    }

    #[test]
    fn get_json_path_returns_a_nested_field() {
        let val: serde_json::Value = serde_json::json!({"address": {"city": "Rome"}});
        let path = parse_json_path("$.address.city").unwrap();
        assert_eq!(get_json_path(&val, &path), Some(&serde_json::json!("Rome")));
    }

    #[test]
    fn get_json_path_returns_none_for_a_missing_field() {
        let val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.surname").unwrap();
        assert_eq!(get_json_path(&val, &path), None);
    }

    #[test]
    fn get_json_path_returns_an_array_element() {
        let val: serde_json::Value = serde_json::json!({"tag": ["dev", "rust"]});
        let path = parse_json_path("$.tag[1]").unwrap();
        assert_eq!(get_json_path(&val, &path), Some(&serde_json::json!("rust")));
    }

    #[test]
    fn get_json_path_rejects_an_out_of_range_index() {
        let val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag[5]").unwrap();
        assert_eq!(get_json_path(&val, &path), None);
    }

    #[test]
    fn test_get_json_path_wrong_type_none() {
        // Array indexing is invalid on an object.
        let val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.name[0]").unwrap();
        assert_eq!(get_json_path(&val, &path), None);
    }

    #[test]
    fn set_json_path_replaces_the_complete_document() {
        let mut val: serde_json::Value = serde_json::json!({"old": true});
        let path = parse_json_path("$").unwrap();
        assert!(set_json_path(
            &mut val,
            &path,
            serde_json::json!({"new": true})
        ));
        assert_eq!(val, serde_json::json!({"new": true}));
    }

    #[test]
    fn set_json_path_replaces_an_existing_field() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.name").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!("Ahmed")));
        assert_eq!(val, serde_json::json!({"name": "Ahmed"}));
    }

    #[test]
    fn set_json_path_adds_a_field_to_an_existing_object() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.age").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!(18)));
        assert_eq!(val, serde_json::json!({"name": "Marco", "age": 18}));
    }

    #[test]
    fn set_json_path_rejects_a_missing_parent() {
        // Intermediate objects are not created automatically.
        let mut val: serde_json::Value = serde_json::json!({});
        let path = parse_json_path("$.a.b.c").unwrap();
        assert!(!set_json_path(&mut val, &path, serde_json::json!(1)));
    }

    #[test]
    fn set_json_path_replaces_an_existing_array_element() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev", "rust"]});
        let path = parse_json_path("$.tag[0]").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!("go")));
        assert_eq!(val, serde_json::json!({"tag": ["go", "rust"]}));
    }

    #[test]
    fn set_json_path_appends_at_the_array_end() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag[1]").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!("rust")));
        assert_eq!(val, serde_json::json!({"tag": ["dev", "rust"]}));
    }

    #[test]
    fn set_json_path_rejects_an_index_past_the_array_end() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag[5]").unwrap();
        assert!(!set_json_path(&mut val, &path, serde_json::json!("x")));
    }

    #[test]
    fn delete_json_path_removes_a_field() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco", "age": 18});
        let path = parse_json_path("$.age").unwrap();
        assert!(delete_json_path(&mut val, &path));
        assert_eq!(val, serde_json::json!({"name": "Marco"}));
    }

    #[test]
    fn delete_json_path_removes_an_array_element() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev", "rust"]});
        let path = parse_json_path("$.tag[0]").unwrap();
        assert!(delete_json_path(&mut val, &path));
        assert_eq!(val, serde_json::json!({"tag": ["rust"]}));
    }

    #[test]
    fn delete_json_path_returns_false_for_a_missing_field() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.surname").unwrap();
        assert!(!delete_json_path(&mut val, &path));
    }

    #[test]
    fn delete_json_path_rejects_the_document_root() {
        // Root deletion is handled as a normal key deletion.
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$").unwrap();
        assert!(!delete_json_path(&mut val, &path));
    }
    // ============================================================
    // JSON NUMINCRBY / ARRAPPEND
    // ============================================================

    #[test]
    fn json_numincrby_updates_an_integer() {
        let mut val: serde_json::Value = serde_json::json!({"visits": 5});
        let path = parse_json_path("$.visits").unwrap();
        let result = numincrby_json_path(&mut val, &path, 3.0);
        assert_eq!(result, Ok(8.0));
        assert_eq!(val, serde_json::json!({"visits": 8}));
    }

    #[test]
    fn test_numincrby_json_path_with_negative_delta() {
        let mut val: serde_json::Value = serde_json::json!({"balance": 10});
        let path = parse_json_path("$.balance").unwrap();
        let result = numincrby_json_path(&mut val, &path, -3.0);
        assert_eq!(result, Ok(7.0));
    }

    #[test]
    fn test_numincrby_json_path_with_float_values() {
        let mut val: serde_json::Value = serde_json::json!({"price": 9.5});
        let path = parse_json_path("$.price").unwrap();
        let result = numincrby_json_path(&mut val, &path, 0.5);
        assert_eq!(result, Ok(10.0));
    }

    #[test]
    fn test_numincrby_json_path_with_string_value_fails() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.name").unwrap();
        assert!(numincrby_json_path(&mut val, &path, 1.0).is_err());
    }

    #[test]
    fn json_numincrby_rejects_a_missing_path() {
        let mut val: serde_json::Value = serde_json::json!({});
        let path = parse_json_path("$.counter").unwrap();
        assert!(numincrby_json_path(&mut val, &path, 1.0).is_err());
    }

    #[test]
    fn json_arrappend_updates_an_existing_array() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag").unwrap();
        let result = arrappend_json_path(&mut val, &path, serde_json::json!("rust"));
        assert_eq!(result, Ok(2));
        assert_eq!(val, serde_json::json!({"tag": ["dev", "rust"]}));
    }

    #[test]
    fn json_arrappend_updates_an_empty_array() {
        let mut val: serde_json::Value = serde_json::json!({"tag": []});
        let path = parse_json_path("$.tag").unwrap();
        let result = arrappend_json_path(&mut val, &path, serde_json::json!("primo"));
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn json_arrappend_rejects_a_non_array() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.name").unwrap();
        assert!(arrappend_json_path(&mut val, &path, serde_json::json!("x")).is_err());
    }

    #[test]
    fn json_arrappend_rejects_a_missing_path() {
        let mut val: serde_json::Value = serde_json::json!({});
        let path = parse_json_path("$.tag").unwrap();
        assert!(arrappend_json_path(&mut val, &path, serde_json::json!("x")).is_err());
    }
}
