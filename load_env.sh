#!/bin/bash
# Load environment variables from .env file
# Usage: source load_env.sh

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found!"
    echo "Creating .env from .env.example..."
    cp .env.example .env
    echo "✅ .env file created. Please update it with your actual values."
fi

# Load environment variables
echo "Loading environment variables from .env..."

# Export each line that's not a comment or empty
while IFS= read -r line; do
    # Skip empty lines and comments
    if [[ -z "$line" ]] || [[ "$line" =~ ^[[:space:]]*# ]]; then
        continue
    fi
    
    # Export the variable
    export "$line"
    
    # Show what was loaded (hide sensitive values)
    var_name=$(echo "$line" | cut -d'=' -f1)
    if [[ "$var_name" == *"PASSWORD"* ]] || [[ "$var_name" == *"SECRET"* ]] || [[ "$var_name" == *"TOKEN"* ]]; then
        echo "  ✓ $var_name=***"
    else
        echo "  ✓ $line"
    fi
done < .env

echo ""
echo "✅ Environment variables loaded successfully!"
echo ""
echo "Loaded variables:"
echo "  - KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS}"
echo "  - KAFKA_INPUT_TOPIC: ${KAFKA_INPUT_TOPIC}"
echo "  - KAFKA_OUTPUT_TOPIC: ${KAFKA_OUTPUT_TOPIC}"
echo "  - API_BASE_URL: ${API_BASE_URL}"
echo "  - LOG_LEVEL: ${LOG_LEVEL}"
echo ""
echo "Usage: source load_env.sh"
