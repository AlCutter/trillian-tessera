terraform {
  source = "${get_repo_root()}/deployment/modules//example-gcp"
}

locals {
  project_id = get_env("GOOGLE_PROJECT", "trillian-tessera")
  location   = get_env("GOOGLE_REGION", "us-central1")
  base_name   = get_env("TESSERA_BASE_NAME", "example-gcp")
  env         = path_relative_to_include()
}

remote_state {
  backend = "gcs"

  config = {
    project  = local.project_id
    location = local.location
    bucket   = "${local.project_id}-${local.base_name}-${local.env}-terraform-state"
    prefix   = "${path_relative_to_include()}/terraform.tfstate"

    gcs_bucket_labels = {
      name  = "terraform_state_storage"
    }
  }
}
