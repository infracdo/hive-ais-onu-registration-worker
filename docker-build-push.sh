#!/bin/bash
# Docker Build and Push Script for ONU Registration Worker

set -e

# Configuration
DEFAULT_DOCKER_HUB_USERNAME="marcandres888"
IMAGE_NAME="onu-registration-worker"
DOCKER_HUB_USERNAME="${1:-$DEFAULT_DOCKER_HUB_USERNAME}"
TAG="${2:-latest}"
FULL_IMAGE_NAME="${DOCKER_HUB_USERNAME}/${IMAGE_NAME}:${TAG}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${GREEN}=== ONU Registration Worker - Docker Build & Push ===${NC}"
echo -e "${CYAN}Docker Hub Username: ${DOCKER_HUB_USERNAME}${NC}"
echo -e "${CYAN}Image Name: ${IMAGE_NAME}${NC}"
echo -e "${CYAN}Tag: ${TAG}${NC}"
echo -e "${CYAN}Full Image Name: ${FULL_IMAGE_NAME}${NC}"
echo ""

# Check Docker
echo -e "${YELLOW}Checking Docker status...${NC}"
if ! docker version >/dev/null 2>&1; then
    echo -e "${RED}✗ Docker is not running${NC}"
    echo -e "${YELLOW}Attempting to start Docker...${NC}"
    if sudo service docker start >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Docker started successfully${NC}"
        sleep 2
    else
        echo -e "${RED}✗ Error: Could not start Docker.${NC}"
        exit 1
    fi
fi

DOCKER_VERSION=$(docker version --format "{{.Server.Version}}" 2>/dev/null)
echo -e "${GREEN}✓ Docker is running (version: ${DOCKER_VERSION})${NC}"

# Check Docker Hub login
echo -e "${YELLOW}Checking Docker Hub authentication...${NC}"
if [ -f ~/.docker/config.json ] && grep -q "auths" ~/.docker/config.json 2>/dev/null; then
    echo -e "${GREEN}✓ Docker credentials found${NC}"
else
    echo -e "${YELLOW}Attempting to login...${NC}"
    if docker login; then
        echo -e "${GREEN}✓ Successfully logged in${NC}"
    else
        echo -e "${RED}✗ Login failed${NC}"
        exit 1
    fi
fi

# Build image
echo ""
echo -e "${YELLOW}Building Docker image...${NC}"
if docker build -t "$FULL_IMAGE_NAME" .; then
    echo -e "${GREEN}✓ Docker build successful!${NC}"
else
    echo -e "${RED}✗ Docker build failed!${NC}"
    exit 1
fi

# Show image details
echo ""
docker images "$FULL_IMAGE_NAME" --format "table {{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.Size}}"

# Push to Docker Hub
echo ""
read -p "Push image to Docker Hub? (Y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    echo -e "${YELLOW}Pushing to Docker Hub...${NC}"
    if docker push "$FULL_IMAGE_NAME"; then
        echo -e "${GREEN}✓ Successfully pushed to Docker Hub!${NC}"
        echo ""
        echo -e "${GREEN}=== Deployment Information ===${NC}"
        echo -e "${CYAN}Image URL: ${FULL_IMAGE_NAME}${NC}"
        echo -e "${CYAN}Docker Hub URL: https://hub.docker.com/r/${DOCKER_HUB_USERNAME}/${IMAGE_NAME}${NC}"
    else
        echo -e "${RED}✗ Docker push failed!${NC}"
        exit 1
    fi
fi

echo ""
echo -e "${GREEN}=== Build Complete ===${NC}"
