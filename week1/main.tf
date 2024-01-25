terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "2.25.2"
    }
  }
}

provider "kubernetes" {
  config_path    = "~/.kube/config"
}

resource "kubernetes_namespace" "example" {
  metadata {
    name = "de-zoomcamp-namespace-week1"
  }
}

resource "kubernetes_deployment" "example" {
  metadata {
    name      = "bigquery-emulator"
    namespace = kubernetes_namespace.example.metadata[0].name
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "bigquery-emulator"
      }
    }

    template {
      metadata {
        labels = {
          app = "bigquery-emulator"
        }
      }

      spec {
        container {
          image = "goccy/bigquery-emulator"
          name  = "bigquery-emulator"

          port {
            container_port = 8086
          }
        }
      }
    }
  }
}
