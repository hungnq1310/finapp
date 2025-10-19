#!/bin/bash

# MongoDB Switch Script for Financial News App
# Usage: ./scripts/switch-mongo.sh [local|atlas]

set -e

ENV_FILE=".env"
BACKUP_FILE=".env.backup"

case "$1" in
    "local")
        echo "🔄 Switching to local MongoDB..."
        if [ -f "$ENV_FILE" ]; then
            cp "$ENV_FILE" "$BACKUP_FILE"
        fi
        cp ".env.example" "$ENV_FILE"
        echo "✅ Switched to local MongoDB (localhost:27017)"
        echo "📝 Make sure MongoDB container is running: docker compose up mongo -d"
        ;;
    
    "atlas")
        echo "🔄 Switching to MongoDB Atlas..."
        if [ -f "$ENV_FILE" ]; then
            cp "$ENV_FILE" "$BACKUP_FILE"
        fi
        if [ -f ".env.atlas" ]; then
            cp ".env.atlas" "$ENV_FILE"
            echo "✅ Switched to MongoDB Atlas"
            echo "⚠️  Make sure to update .env.atlas with your Atlas credentials:"
            echo "   - Replace <username> with your Atlas username"
            echo "   - Replace <password> with your Atlas password"
            echo "   - Replace <cluster-url> with your Atlas cluster URL"
        else
            echo "❌ .env.atlas file not found!"
            exit 1
        fi
        ;;
    
    "restore")
        echo "🔄 Restoring previous configuration..."
        if [ -f "$BACKUP_FILE" ]; then
            cp "$BACKUP_FILE" "$ENV_FILE"
            echo "✅ Configuration restored from backup"
        else
            echo "❌ No backup file found!"
            exit 1
        fi
        ;;
    
    *)
        echo "Usage: $0 [local|atlas|restore]"
        echo ""
        echo "Commands:"
        echo "  local   - Switch to local MongoDB (Docker)"
        echo "  atlas   - Switch to MongoDB Atlas"
        echo "  restore - Restore previous configuration"
        echo ""
        echo "Examples:"
        echo "  $0 local      # Use local MongoDB"
        echo "  $0 atlas      # Use MongoDB Atlas"
        echo "  $0 restore    # Restore backup"
        exit 1
        ;;
esac

echo "🔄 Restarting application with new configuration..."
docker compose up finapp-api --force-recreate -d