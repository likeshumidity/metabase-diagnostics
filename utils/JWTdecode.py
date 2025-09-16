#!/usr/bin/env python3

"""
JWTdecode - Decodes JWT tokens from STDIN to STDOUT
Usage: echo "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..." | JWTdecode
"""

import sys
import json
import base64

def pad_base64(s):
    """Add padding to base64 string if needed"""
    return s + '=' * (4 - len(s) % 4)

def decode_jwt_part(encoded_part):
    """Decode a JWT part (header or payload)"""
    try:
        # Add padding and decode
        padded = pad_base64(encoded_part)
        decoded_bytes = base64.urlsafe_b64decode(padded)
        decoded_str = decoded_bytes.decode('utf-8')
        return json.loads(decoded_str)
    except Exception as e:
        return f"Error decoding: {e}"

def main():
    # Read JWT from stdin
    jwt_token = sys.stdin.read().strip()

    if not jwt_token:
        print("Error: No JWT token provided", file=sys.stderr)
        sys.exit(1)

    # Split JWT into parts
    parts = jwt_token.split('.')

    if len(parts) != 3:
        print("Error: Invalid JWT format. Expected 3 parts separated by dots.", file=sys.stderr)
        sys.exit(1)

    header, payload, signature = parts

    print("=== JWT HEADER ===")
    header_decoded = decode_jwt_part(header)
    print(json.dumps(header_decoded, indent=2))

    print("\n=== JWT PAYLOAD ===")
    payload_decoded = decode_jwt_part(payload)
    print(json.dumps(payload_decoded, indent=2))

    print(f"\n=== JWT SIGNATURE ===")
    print(f"Signature (base64url): {signature}")
    print("Note: Signature verification requires the secret key")

if __name__ == "__main__":
    main()