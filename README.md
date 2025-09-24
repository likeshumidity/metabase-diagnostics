# Metabase Diagnostics

A comprehensive suite of diagnostic tools for troubleshooting Metabase instances, built with Clojure.

## Overview

Metabase Diagnostics provides a CLI framework for running various diagnostic checks against Metabase installations. The tool uses a modular architecture with shared libraries for common functionality like API client, logging, and output formatting.

## Architecture

- **Clojure-based CLI**: Uses `tools.cli` for argument parsing and command orchestration
- **Monorepo Structure**: Shared libraries and individual diagnostic tools
- **API Key Authentication**: Secure connection to Metabase instances via X-API-Key header
- **Multiple Output Formats**: JSON, EDN, table, CSV, and summary formats
- **Structured Logging**: Context-aware logging with configurable levels

## Installation

### Prerequisites

- Java 11 or higher
- Clojure CLI tools (`clojure` command)

### Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd metabase-diagnostics
   ```

2. **Verify installation:**
   ```bash
   clojure -M -m metabase-diagnostics.cli --version
   ```

## Configuration

### Metabase API Key

To use the diagnostic tools, you'll need a Metabase API key:

1. **Log into your Metabase instance as an admin**
2. **Go to Admin Settings > API Keys**
3. **Create a new API key** with appropriate permissions
4. **Copy the generated API key** (starts with `mb_`)

### Environment Variables (Optional)

You can set default values using environment variables:

```bash
export METABASE_URL="http://localhost:3000"
export METABASE_API_KEY="mb_your_api_key_here"
```

## Building and Running Uberjar

### Building Uberjar

```bash
clojure -X:build uber
```

Builds uberjar in `target/` path.

### Running Uberjar

```bash
java -jar target/metabase-diagnostics-x.x.x-standalone.jar
```

Replace `x.x.x` with the version number.


## Usage

### Basic Command Structure

```bash
clojure -M -m metabase-diagnostics.cli [global-options] <command> [command-options]
```

### Global Options

- `-h, --help` - Show help
- `-v, --version` - Show version
- `-V, --verbose` - Enable verbose logging
- `--log-level LEVEL` - Set log level (trace|debug|info|warn|error)
- `--format FORMAT` - Output format (json|edn|table|csv|summary)
- `-q, --quiet` - Suppress non-essential output

### Available Commands

#### API Diagnostics

Check Metabase instance health via API:

```bash
# Basic usage
clojure -M -m metabase-diagnostics.cli api-diagnostics \
  --base-url http://localhost:3000 \
  --api-key mb_your_api_key_here

# With custom options
clojure -M -m metabase-diagnostics.cli \
  --format json \
  --log-level warn \
  api-diagnostics \
  --base-url http://localhost:3000 \
  --api-key mb_your_api_key_here \
  --timeout 60 \
  --retries 5 \
  --no-check-databases
```

**API Diagnostics Options:**
- `-u, --base-url URL` - Metabase base URL (required)
- `-k, --api-key KEY` - Metabase API key (required)
- `--timeout SECONDS` - Request timeout (default: 30)
- `--retries COUNT` - Number of retries (default: 3)
- `--[no-]check-health` - Check instance health (default: true)
- `--[no-]check-databases` - Check database connectivity (default: true)
- `--[no-]check-collections` - Check collections access (default: true)

## Examples

### Quick Health Check

```bash
# JSON output for easy parsing
clojure -M -m metabase-diagnostics.cli --format json api-diagnostics \
  -u http://localhost:3000 \
  -k mb_cSlhxr3qVOTJJlI4mX33f27JFFcQNmW1hEN13Zn8euw=
```

### Verbose Debugging

```bash
# Enable verbose logging for troubleshooting
clojure -M -m metabase-diagnostics.cli --verbose api-diagnostics \
  -u http://localhost:3000 \
  -k mb_your_api_key_here
```

### Quiet Monitoring

```bash
# Minimal output for monitoring scripts
clojure -M -m metabase-diagnostics.cli --quiet --format json api-diagnostics \
  -u http://localhost:3000 \
  -k mb_your_api_key_here 2>/dev/null
```

## Output Formats

### Summary (Default)
Human-readable summary format:
```
Summary:
  total-tests: 4
  passed-tests: 4
  failed-tests: 0
  success: true
```

### JSON
Machine-readable JSON format:
```json
{
  "summary": {
    "total-tests": 4,
    "passed-tests": 4,
    "failed-tests": 0,
    "success": true
  },
  "results": [...]
}
```

### EDN
Clojure data format:
```clojure
{:summary {:total-tests 4 :passed-tests 4 :failed-tests 0 :success true}
 :results [...]}
```

## Development

### Project Structure

```
metabase-diagnostics/
├── deps.edn                 # Root project dependencies
├── shared/                  # Shared libraries
│   └── src/metabase_diagnostics/
│       ├── api/
│       │   └── client.clj   # API client library
│       ├── logging/
│       │   └── core.clj     # Logging utilities
│       └── output/
│           └── formatter.clj # Output formatting
└── tools/                   # Diagnostic tools
    └── src/metabase_diagnostics/
        ├── cli.clj          # Main CLI orchestrator
        └── tools/
            └── api_diagnostics.clj # API diagnostic tool
```

### Adding New Diagnostic Tools

1. **Create tool module** in `tools/src/metabase_diagnostics/tools/`
2. **Implement required functions:**
   - `show-help` - Display tool-specific help
   - `run-validation` - Main entry point matching CLI interface
3. **Register tool** in `cli.clj` `AVAILABLE_TOOLS` registry
4. **Follow shared patterns** for option parsing and output formatting

### Example Tool Template

```clojure
(ns metabase-diagnostics.tools.my-tool
  (:require [clojure.tools.cli :as cli]))

(def TOOL_OPTIONS
  [["-h" "--help" "Show help for my-tool"]
   ["-u" "--base-url URL" "Metabase base URL" :required true]])

(defn show-help []
  (println "My Tool - Description of what this tool does"))

(defn run-validation [{:keys [tool-args global-options]}]
  (let [parsed (cli/parse-opts tool-args TOOL_OPTIONS)
        opts (:options parsed)]
    (if (:help opts)
      (do (show-help) {:action :help-shown})
      ;; Implement tool logic here
      {:summary {:success true} :results []})))
```

## Troubleshooting

### Common Issues

1. **"Unknown tool" error**: Verify tool is registered in `AVAILABLE_TOOLS`
2. **API authentication errors**: Check API key permissions and format
3. **Connection timeouts**: Increase `--timeout` value or check network connectivity
4. **JSON parsing errors**: Verify Metabase instance is returning valid JSON responses

### Debug Mode

Enable verbose logging to see detailed API calls and internal operations:

```bash
clojure -M -m metabase-diagnostics.cli --verbose --log-level debug api-diagnostics [options]
```

## Contributing

### Development Setup

1. Clone repository
2. Run `clojure -X:dev` to start REPL
3. Make changes and test with `clojure -M -m metabase-diagnostics.cli --help`

### Code Style

- Follow Clojure conventions
- Extract constants using ALL_CAPS_WITH_UNDERSCORES
- Use tools.cli for argument parsing
- Return structured data from diagnostic functions
- Include proper error handling and logging

## License

[Add appropriate license information]

## Support

For issues and feature requests, please [create an issue](link-to-issues) or consult the troubleshooting section above.
