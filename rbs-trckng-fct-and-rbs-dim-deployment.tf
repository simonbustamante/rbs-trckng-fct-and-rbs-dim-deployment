
#### #### #### #### #### #### #### #### #### #### #### #### 
#### #### #### #### #### #### #### #### #### #### #### #### 
#### #### #### #### #### #### #### #### #### #### #### #### 
#### aws.bi.LakeH.hq.prd #525196274797 #### #### #### #####
#### #### #### #### #### #### #### #### #### #### #### #### 
#### #### #### #### #### #### #### #### #### #### #### #### 
#### #### #### #### #### #### #### #### #### #### #### #### 

locals {
  profile = "525196274797_AWSAdministratorAccess"
  region = "us-east-1"
  file_cp_to_bucket_cntry = "DTL.CNTRY_DIM.csv"
  file_cp_to_bucket_ext = "glue_hq_ntwrk_zeus_init_rbs_and_external_dims_prd"
  file_cp_to_bucket_bo = "glu_hq_ntwrk_bo_rbs_dim_to_rbs_trckng_fct_prd_001"
  file_cp_to_bucket_gt = "glu_hq_ntwrk_gt_rbs_dim_to_rbs_trckng_fct_prd_001"
  file_cp_to_bucket_hn = "glu_hq_ntwrk_hn_rbs_dim_to_rbs_trckng_fct_prd_001"
  file_cp_to_bucket_rbs_dim = "glu_hq_ntwrk_rbs_dim_prd"
  bucket_install_script = "s3-hq-raw-prd-intec"
  prefix_install_script_ext = "app_glue_hq_ntwrk_zeus_init_rbs_and_external_dims_prd"
  prefix_install_script_bo = "app_glu_hq_ntwrk_bo_rbs_dim_to_rbs_trckng_fct_prd_001"
  prefix_install_script_gt = "app_glu_hq_ntwrk_gt_rbs_dim_to_rbs_trckng_fct_prd_001"
  prefix_install_script_hn = "app_glu_hq_ntwrk_hn_rbs_dim_to_rbs_trckng_fct_prd_001"
  prefix_install_script_rbs_dim = "app_glu_hq_ntwrk_rbs_dim_prd"
  db_target = "hq-anl-prd-ntwrk-link"
  crawler_name_1 = "crwl-hq-anl-prd-ntwrk-zeus-rbs-trckng-fct"
  bucket_to_crawl_1 = "s3-hq-anl-prd-ntwrk"
  prefix_to_crawl_1 = "rbs_trckng_fct"
  crawler_name_2 = "crwl-hq-anl-prd-zeus-rbs-dim"
  bucket_to_crawl_2 = "s3-hq-anl-prd-ntwrk"
  prefix_to_crawl_2 = "zeus_rbs_dim"
  crawler_name_3 = ""
  bucket_to_crawl_3 = "s3-hq-raw-prd-ntwrk"
  prefix_to_crawl_3 = "zeus_cntry_dim"
  svc_role_arn = "arn:aws:iam::525196274797:role/svc-role-data-mic-development-integrations"
  step_function_name = "stp-fnc-all-countries-ntwrk-zeus-rbs-dim-to-rbs-trcking-fct-prd"
  step_function_path = "stp-fnc-all-countries-ntwrk-zeus-rbs-dim-to-rbs-trcking-fct-prd.json"
  sns_name = "zeus-hq-rbs-trckng-fct-and-rbs-dim-prd"
}

# Lista de correos electrónicos
# variable "emails" {
#   description = "Lista de correos electrónicos para suscripción."
#   type        = list(string)
#   default     = [
#     "simon.bustamante@millicom.com", 
#     #"email2@gmail.com", 
#   ]
# }



# AWS PROFILE
provider "aws" {
  alias = "aws-bi-LakeH-hq-prd"
  profile = local.profile
  region  = local.region
}

# ## INSTALAR SCRIPT

# borrar codigo viejo y copiar el archivo al bucket
# se ejecuta todo el tiempo 

# HQ INIT EXTERNAL DIMS
resource "null_resource" "copy_source_code_ext" {
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = "aws s3 cp ${local.file_cp_to_bucket_ext}.py s3://${local.bucket_install_script}/${local.prefix_install_script_ext}/ --profile ${local.profile}"
  }
}

# BO 
resource "null_resource" "copy_source_code_bo" {
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = "aws s3 cp ${local.file_cp_to_bucket_bo}.py s3://${local.bucket_install_script}/${local.prefix_install_script_bo}/ --profile ${local.profile}"
  }
}

# GT 
resource "null_resource" "copy_source_code_gt" {
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = "aws s3 cp ${local.file_cp_to_bucket_gt}.py s3://${local.bucket_install_script}/${local.prefix_install_script_gt}/ --profile ${local.profile}"
  }
}

# HN
resource "null_resource" "copy_source_code_hn" {
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = "aws s3 cp ${local.file_cp_to_bucket_hn}.py s3://${local.bucket_install_script}/${local.prefix_install_script_hn}/ --profile ${local.profile}"
  }
}

# HQ RBS DIM
resource "null_resource" "copy_source_code_rbs_dim" {
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = "aws s3 cp ${local.file_cp_to_bucket_rbs_dim}.py s3://${local.bucket_install_script}/${local.prefix_install_script_rbs_dim}/ --profile ${local.profile}"
  }
}

# copy cntry dim 
resource "null_resource" "copy_source_cntry_dim" {
  depends_on = [aws_glue_job.create_job_ext_init]
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = "aws s3 cp ${local.file_cp_to_bucket_cntry} s3://${local.bucket_to_crawl_3}/${local.prefix_to_crawl_3}/ --profile ${local.profile}"
  }
}
# ejecutar job de inicializar tablas externas
resource "null_resource" "execute_init_ext" {
  depends_on = [aws_glue_job.create_job_ext_init]
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = "aws glue start-job-run --job-name ${aws_glue_job.create_job_ext_init.name} --profile ${local.profile}"
  }
}

### CREAR JOBS EN GLUE

# HQ EXTERNAL INIT JOBS
resource "aws_glue_job" "create_job_ext_init" {
  provider = aws.aws-bi-LakeH-hq-prd
  name     = local.file_cp_to_bucket_ext
  role_arn = local.svc_role_arn
  glue_version = "4.0"

  command {
    script_location = "s3://${local.bucket_install_script}/${local.prefix_install_script_ext}/${local.file_cp_to_bucket_ext}.py" 
    python_version  = "3"                          
  }

  default_arguments = {
    "--TempDir"             = "s3://${local.bucket_install_script}/${local.prefix_install_script_ext}/temp-dir/"
    "--job-bookmark-option" = "job-bookmark-enable"
    #"--extra-py-files"      = "s3://${local.bucket_to_create}/extra-files.zip"  # Si necesitas archivos adicionales
  }

  max_retries  = 0      # Número de reintentos en caso de fallo
  timeout      = 60     # Tiempo máximo de ejecución en minutos
  number_of_workers = 5 # numero de workers del job
  worker_type = "G.1X"  # tipo de worker
}



#BO
resource "aws_glue_job" "create_job_rbs_trckng_fct_bo" {
  provider = aws.aws-bi-LakeH-hq-prd
  name     = local.file_cp_to_bucket_bo
  role_arn = local.svc_role_arn
  glue_version = "4.0"

  command {
    script_location = "s3://${local.bucket_install_script}/${local.prefix_install_script_bo}/${local.file_cp_to_bucket_bo}.py" 
    python_version  = "3"                          
  }

  default_arguments = {
    "--TempDir"             = "s3://${local.bucket_install_script}/${local.prefix_install_script_bo}/temp-dir/"
    "--job-bookmark-option" = "job-bookmark-enable"
    #"--extra-py-files"      = "s3://${local.bucket_to_create}/extra-files.zip"  # Si necesitas archivos adicionales
  }

  max_retries  = 0      # Número de reintentos en caso de fallo
  timeout      = 60     # Tiempo máximo de ejecución en minutos
  number_of_workers = 5 # numero de workers del job
  worker_type = "G.1X"  # tipo de worker
}

#GT
resource "aws_glue_job" "create_job_rbs_trckng_fct_gt" {
  provider = aws.aws-bi-LakeH-hq-prd
  name     = local.file_cp_to_bucket_gt
  role_arn = local.svc_role_arn
  glue_version = "4.0"

  command {
    script_location = "s3://${local.bucket_install_script}/${local.prefix_install_script_gt}/${local.file_cp_to_bucket_gt}.py" 
    python_version  = "3"                          
  }

  default_arguments = {
    "--TempDir"             = "s3://${local.bucket_install_script}/${local.prefix_install_script_gt}/temp-dir/"
    "--job-bookmark-option" = "job-bookmark-enable"
    #"--extra-py-files"      = "s3://${local.bucket_to_create}/extra-files.zip"  # Si necesitas archivos adicionales
  }

  max_retries  = 0      # Número de reintentos en caso de fallo
  timeout      = 60     # Tiempo máximo de ejecución en minutos
  number_of_workers = 5 # numero de workers del job
  worker_type = "G.1X"  # tipo de worker
}

#HN
resource "aws_glue_job" "create_job_rbs_trckng_fct_hn" {
  provider = aws.aws-bi-LakeH-hq-prd
  name     = local.file_cp_to_bucket_hn
  role_arn = local.svc_role_arn
  glue_version = "4.0"

  command {
    script_location = "s3://${local.bucket_install_script}/${local.prefix_install_script_hn}/${local.file_cp_to_bucket_hn}.py" 
    python_version  = "3"                          
  }

  default_arguments = {
    "--TempDir"             = "s3://${local.bucket_install_script}/${local.prefix_install_script_hn}/temp-dir/"
    "--job-bookmark-option" = "job-bookmark-enable"
    #"--extra-py-files"      = "s3://${local.bucket_to_create}/extra-files.zip"  # Si necesitas archivos adicionales
  }

  max_retries  = 0     # Número de reintentos en caso de fallo
  timeout      = 60     # Tiempo máximo de ejecución en minutos
  number_of_workers = 5 # numero de workers del job
  worker_type = "G.1X"  # tipo de worker
}

#HQ RBS DIM
resource "aws_glue_job" "create_job_rbs_dim" {
  provider = aws.aws-bi-LakeH-hq-prd
  name     = local.file_cp_to_bucket_rbs_dim
  role_arn = local.svc_role_arn
  glue_version = "4.0"

  command {
    script_location = "s3://${local.bucket_install_script}/${local.prefix_install_script_rbs_dim}/${local.file_cp_to_bucket_rbs_dim}.py" 
    python_version  = "3"                          
  }

  default_arguments = {
    "--TempDir"             = "s3://${local.bucket_install_script}/${local.prefix_install_script_rbs_dim}/temp-dir/"
    "--job-bookmark-option" = "job-bookmark-enable"
    #"--extra-py-files"      = "s3://${local.bucket_to_create}/extra-files.zip"  # Si necesitas archivos adicionales
  }

  max_retries  = 0      # Número de reintentos en caso de fallo
  timeout      = 60     # Tiempo máximo de ejecución en minutos
  number_of_workers = 5 # numero de workers del job
  worker_type = "G.1X"  # tipo de worker
}


### CONFIGURAR CRAWLERS
#RBS_TRCKNG_FCT
resource "aws_glue_crawler" "rbs_trckng_fct_crawler" {
  provider = aws.aws-bi-LakeH-hq-prd
  name          = local.crawler_name_1
  role          = local.svc_role_arn  # Asegúrate de reemplazar esto con el ARN de tu rol de IAM para Glue

  database_name = "${local.db_target}"  # Reemplaza con el nombre de tu base de datos de Glue

  s3_target {
    path = "s3://${local.bucket_to_crawl_1}/${local.prefix_to_crawl_1}/"
  }
}
#RBS_DIM
resource "aws_glue_crawler" "rbs_dim_crawler" {
  provider = aws.aws-bi-LakeH-hq-prd
  name          = local.crawler_name_2
  role          = local.svc_role_arn  # Asegúrate de reemplazar esto con el ARN de tu rol de IAM para Glue

  database_name = "${local.db_target}"  # Reemplaza con el nombre de tu base de datos de Glue

  s3_target {
    path = "s3://${local.bucket_to_crawl_2}/${local.prefix_to_crawl_2}/"
  }
}

#### CONFIGURAR STEP FUNCTION

resource "aws_sfn_state_machine" "rbs_trckng_fct_and_rbs_dim_state_machine" {
  provider = aws.aws-bi-LakeH-hq-prd
  name     = local.step_function_name
  role_arn = local.svc_role_arn

  # Lee la definición de la máquina de estados del archivo JSON
  definition = file("${local.step_function_path}")
}


# # NOTIFICACIONES SNS

# resource "aws_sns_topic" "logging_error_topic_prd" {
#   provider = aws.aws-bi-LakeH-hq-prd
#   name = local.sns_name
# }

# resource "aws_sns_topic_subscription" "elogging_error_subscription_prd" {
#   provider = aws.aws-bi-LakeH-hq-prd
#   for_each  = toset(var.emails)

#   topic_arn = aws_sns_topic.logging_error_topic_prd.arn
#   protocol  = "email"
#   endpoint  = each.value
# }

# # CREAR LAMBDA



# resource "aws_lambda_function" "lambda_function" {
#   provider = aws.aws-bi-LakeH-hq-prd
#   function_name = local.file_cp_to_bucket2
#   role          = local.svc_role_arn

#   handler       = "${local.file_cp_to_bucket2}.lambda_handler" # Asegúrate de cambiar esto al nombre de tu archivo y método handler
#   runtime       = "python3.11" # Asegúrate de usar la versión correcta de Python

#   s3_bucket     = local.bucket_install_script
#   s3_key        = "${local.prefix_install_script2}/${local.file_cp_to_bucket2}.zip"

#   # Configuraciones adicionales como variables de entorno, memoria, tiempo de ejecución máximo, etc.
#   timeout       = 15 #segundos
# }


