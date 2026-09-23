terraform {
  required_version = ">= 1.5.0"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}

provider "kubernetes" {
  config_path = "~/.kube/config"
}

# 1. Namespaces
resource "kubernetes_namespace" "ai_agents" {
  metadata {
    name = "ai-agents"
    labels = {
      istio-injection = "enabled"
    }
  }
}

resource "kubernetes_namespace" "data_platform" {
  metadata {
    name = "data-platform"
  }
}

resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = "monitoring"
  }
}

# 2. Service Accounts для микросервисов
resource "kubernetes_service_account" "ingestion_sa" {
  metadata {
    name      = "ingestion-sa"
    namespace = kubernetes_namespace.ai_agents.metadata[0].name
  }
}

resource "kubernetes_service_account" "analyzer_sa" {
  metadata {
    name      = "analyzer-sa"
    namespace = kubernetes_namespace.ai_agents.metadata[0].name
  }
}

# 3. Базовые секреты (Bootstrap Secrets)
resource "kubernetes_secret" "app_secrets" {
  metadata {
    name      = "aiops-bootstrap-secrets"
    namespace = kubernetes_namespace.ai_agents.metadata[0].name
  }

  data = {
    "DB_PASSWORD" = "postgres"
    "ENVIRONMENT" = "staging"
  }

  type = "Opaque"
}