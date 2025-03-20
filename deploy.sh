#!/bin/bash

set -e 

NAMESPACE="eo-workflow"
REGISTRY="docker.io/davidandw190"  
INGESTION_CONTAINER_IMAGE="${REGISTRY}/stac-ingestion:latest"

echo "[1/7] Creating namespace..."
kubectl apply -f k8s/namespace.yaml

echo "[2/7] Deploying Redis..."
kubectl apply -f k8s/redis/001-configmap.yaml
kubectl apply -f k8s/redis/002-service.yaml
kubectl apply -f k8s/redis/003-deployment.yaml
kubectl apply -f k8s/redis/004-pvc.yaml

echo "Waiting for Redis to be ready..."
kubectl wait --for=condition=available --timeout=60s deployment/redis -n $NAMESPACE || \
    echo "Redis deployment not yet available, continuing anyway..."

echo "[3/7] Deploying MinIO..."
kubectl apply -f k8s/minio/001-configmap.yaml
kubectl apply -f k8s/minio/002-pvc.yaml
kubectl apply -f k8s/minio/003-services.yaml
kubectl apply -f k8s/minio/004-statefulset.yaml

echo "[4/7] Skipping Vault deployment for now..."
# echo "[4/8] Deploying Vault..."
# kubectl apply -f k8s/vault/001-minio-serviceaccount.yaml
# kubectl apply -f k8s/vault/002-minio-clusterrolebinding.yaml
# kubectl apply -f k8s/vault/002-rolebinding.yaml
# kubectl apply -f k8s/vault/002-role.yaml
# kubectl apply -f k8s/vault/003-service.yaml
# kubectl apply -f k8s/vault/004-configmap.yaml
# kubectl apply -f k8s/vault/005-deployment.yaml
# 
# # Wait for Vault to be ready
# echo "Waiting for Vault to be ready..."
# kubectl wait --for=condition=available --timeout=120s deployment/vault -n $NAMESPACE || \
#     echo "Vault deployment not yet available, continuing anyway..."

# # Initialize MinIO credentials in Vault
# echo "[5/8] Initializing MinIO credentials in Vault..."
# VAULT_POD=$(kubectl get pods -n $NAMESPACE -l app=vault -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || echo "")

# if [ -n "$VAULT_POD" ]; then
#     MINIO_USER="miniouser"
#     MINIO_PASSWORD="$(openssl rand -hex 16)"

#     kubectl exec -n $NAMESPACE $VAULT_POD -- /bin/sh -c "
#     set -e
#     export VAULT_ADDR=http://localhost:8200
#     export VAULT_TOKEN=root

#     # Enable KV v2 secrets engine
#     vault secrets enable -path=minio kv-v2 2>/dev/null || echo 'Secrets engine already enabled'

#     # Store MinIO credentials
#     vault kv put minio/data/config MINIO_ROOT_USER=\"$MINIO_USER\" MINIO_ROOT_PASSWORD=\"$MINIO_PASSWORD\"

#     # Configure Kubernetes auth
#     vault auth enable kubernetes 2>/dev/null || echo 'Kubernetes auth already enabled'

#     # Configure the Kubernetes auth method
#     vault write auth/kubernetes/config \
#         kubernetes_host=\"https://\$KUBERNETES_PORT_443_TCP_ADDR:443\" \
#         token_reviewer_jwt=\"\$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)\" \
#         kubernetes_ca_cert=\"\$(cat /var/run/secrets/kubernetes.io/serviceaccount/ca.crt)\" \
#         issuer=\"https://kubernetes.default.svc.cluster.local\"

#     # Create a role for MinIO
#     vault write auth/kubernetes/role/minio-role \
#         bound_service_account_names=minio-vault-auth \
#         bound_service_account_namespaces=$NAMESPACE \
#         policies=default \
#         ttl=1h
#     "

#     echo "MinIO credentials stored in Vault"
#     echo "MinIO username: $MINIO_USER"
#     echo "MinIO password: $MINIO_PASSWORD (save this password!)"
# else
#     echo "Vault pod not found. Skipping credentials initialization."
#     echo "You'll need to manually set up Vault or reconfigure functions to use static credentials."
# fi

echo "[5/7] Deploying Knative Eventing components..."
kubectl apply -f k8s/knative/001-broker.yaml
kubectl apply -f k8s/knative/002-triggers.yaml
kubectl apply -f k8s/knative/003-event-display.yaml 2>/dev/null || echo "Event display deployment skipped"

echo "[6/7] Deploying Ingestion ContainerSource..."
kubectl apply -f k8s/sources/stac-container-secret.yaml
kubectl apply -f k8s/sources/stac-container-cm.yaml

if [[ "$1" == "--build-container" ]]; then
    echo "Building Ingestion container image..."
    cd sources/ingestion
    docker build -t $INGESTION_CONTAINER_IMAGE .
    docker push $INGESTION_CONTAINER_IMAGE
    cd ..
    echo "Ingestion  container image built and pushed"
else
    echo "Skipping Ingestion container build. Use --build-container flag to build it."
    echo "Make sure the image '$INGESTION_CONTAINER_IMAGE' exists in your registry."
fi

kubectl apply -f k8s/sources/ingestion-deployment.yaml

echo "[8/8] Building and deploying Knative functions..."
for func in cog-transformer completion-tracker fmask webhook; do
    echo "Deploying function: $func"
    cd functions/$func
    
    if [ "$func" == "completion-tracker" ]; then
        kubectl create configmap completion-tracker-config \
            --from-literal=REDIS_HOST=redis.$NAMESPACE.svc.cluster.local \
            --from-literal=REDIS_PORT=6379 \
            --from-literal=REDIS_KEY_PREFIX=eo:tracker: \
            --from-literal=REDIS_KEY_EXPIRY=86400 \
            -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
    fi
    
    echo "  Building $func..."
    kn func build
    
    echo "  Deploying $func..."
    kn func deploy
    
    cd ../..
    echo "Function $func deployed"
done

echo "Checking deployment status..."
echo "Knative ContainerSource:"
kubectl get containersources -n $NAMESPACE

echo "Knative Services:"
kubectl get ksvc -n $NAMESPACE

echo "Knative Triggers:"
kubectl get triggers -n $NAMESPACE

echo "MinIO and Redis:"
kubectl get pods -n $NAMESPACE -l 'app in (minio,redis)'

WEBHOOK_URL=$(kubectl get ksvc eo-webhook -n $NAMESPACE -o jsonpath="{.status.url}" 2>/dev/null || echo "Not available yet")

echo "Earth Observation Workflow Deployment Complete!"
echo "To test the workflow, send a request to the webhook:"
echo "Webhook URL: $WEBHOOK_URL"
echo "Example request:"
echo "curl -X POST $WEBHOOK_URL -H \"Content-Type: application/json\" -d '{\"bbox\": [24.55, 47.55, 24.65, 47.65], \"time_range\": \"2024-07-01/2024-07-31\", \"cloud_cover\": 30, \"max_items\": 2}'"

echo "To monitor the workflow:"
echo "1. Check function logs: kubectl logs -n $NAMESPACE -l serving.knative.dev/service=cog-transformer"
echo "2. Monitor events: kubectl logs -n $NAMESPACE -l serving.knative.dev/service=event-monitor"
echo "3. Check STAC container logs: kubectl logs -n $NAMESPACE -l app=stac-ingestion"
echo "4. Access MinIO UI: kubectl port-forward -n $NAMESPACE svc/minio 9001:9001"
echo "   Then open http://localhost:9001 in your browser"