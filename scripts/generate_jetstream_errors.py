#!/usr/bin/env python3
"""
Script to process NATS server errors.json file and generate jetstream_errors.zig
with all JetStream error codes and their mappings.
"""

import json
import sys
from pathlib import Path

def constant_to_error_name(constant: str) -> str:
    """Convert JS error constant to Zig error name"""
    # Remove JS prefix and suffixes
    name = constant
    if name.startswith('JS'):
        name = name[2:]
    # Remove error suffixes (check longer suffixes first)
    if name.endswith('ErrF'):
        name = name[:-4]
    elif name.endswith('Err'):
        name = name[:-3]
    elif name.endswith('F'):
        name = name[:-1]
    
    # Convert from CamelCase to PascalCase (already mostly there)
    return name

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 generate_jetstream_errors.py <errors.json>")
        print("Example: python3 scripts/generate_jetstream_errors.py _refs/nats-server/server/errors.json")
        return 1
    
    # Read the errors.json file
    errors_file = Path(sys.argv[1])
    if not errors_file.exists():
        print(f"Error: {errors_file} not found")
        return 1
    
    with open(errors_file, 'r') as f:
        errors = json.load(f)
    
    # Filter for JetStream errors (error codes 10000-10999)
    js_errors = [err for err in errors if 10000 <= err['error_code'] <= 10999]
    
    # Sort by error code
    js_errors.sort(key=lambda x: x['error_code'])
    
    # Generate Zig code
    zig_content = [
        f"// Generated from {errors_file}",
        "// DO NOT EDIT MANUALLY - regenerate using scripts/generate_jetstream_errors.py",
        "",
        "const std = @import(\"std\");",
        "",
        "/// JetStream error codes and their corresponding Zig error types",
        "pub const JetStreamError = error{",
    ]
    
    # Add error variants
    seen_names = set()
    for err in js_errors:
        error_name = constant_to_error_name(err['constant'])
        # Handle duplicate names by adding suffix
        original_name = error_name
        counter = 1
        while error_name in seen_names:
            error_name = f"{original_name}{counter}"
            counter += 1
        seen_names.add(error_name)
        
        zig_content.append(f"    /// {err['description']} (code: {err['error_code']})")
        zig_content.append(f"    {error_name},")
    
    zig_content.extend([
        "    /// Unknown or unmapped error code",
        "    UnknownError,",
        "};",
        "",
        "/// Map JetStream API error codes to Zig errors",
        "pub fn mapErrorCode(error_code: u32) JetStreamError {",
        "    return switch (error_code) {",
    ])
    
    # Add error code mappings
    seen_names = set()
    for err in js_errors:
        error_name = constant_to_error_name(err['constant'])
        # Handle duplicate names by adding suffix (same logic as above)
        original_name = error_name
        counter = 1
        while error_name in seen_names:
            error_name = f"{original_name}{counter}"
            counter += 1
        seen_names.add(error_name)
        
        zig_content.append(f"        {err['error_code']} => JetStreamError.{error_name},")
    
    zig_content.extend([
        "        else => JetStreamError.UnknownError,",
        "    };",
        "}",
        "",
        f"// Total JetStream errors: {len(js_errors)}",
    ])
    
    # Write the generated file
    output_file = Path('src/jetstream_errors.zig')
    with open(output_file, 'w') as f:
        f.write('\n'.join(zig_content))
    
    print(f"Generated {output_file} with {len(js_errors)} JetStream error codes")
    
    # Print some statistics
    error_codes_by_http = {}
    for err in js_errors:
        http_code = err['code']
        if http_code not in error_codes_by_http:
            error_codes_by_http[http_code] = 0
        error_codes_by_http[http_code] += 1
    
    print(f"Error breakdown by HTTP status code:")
    for http_code in sorted(error_codes_by_http.keys()):
        print(f"  {http_code}: {error_codes_by_http[http_code]} errors")
    
    # Show expected_* related errors
    expected_errors = [err for err in js_errors if 'expected' in err['description'].lower() or 'last' in err['description'].lower() or 'wrong' in err['description'].lower()]
    print(f"\nExpected/validation related errors ({len(expected_errors)}):")
    for err in expected_errors:
        print(f"  {err['error_code']}: {err['description']}")

if __name__ == '__main__':
    exit(main())