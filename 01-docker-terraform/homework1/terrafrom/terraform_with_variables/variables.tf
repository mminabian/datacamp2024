variable "credentials" {
    description = "my credentails path"
    default = "../keys/my_cred.json"
  
}
variable "project"{
  description = "my project id"
  default = "terraform-demo-411721"

}
variable "region" {
    description = "region"
    default = "us-west1"
  
}
variable "location" {
    description = "project location"
    default = "US"
  
}
variable "bq_dataset_name" {
  description = "bigquery dataset name"
  default = "dataset_demo"
}
variable "gcs_bucket_name"{
    description = "MY BUCKET NAME"
    default = "terraform-demo-411721-terra-bucket"
}