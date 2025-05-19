targets::tar_option_set(
  format = "qs",
  packages = c("config", "readr", "urbanplanr")
)

targets::tar_source()

list(
  targets::tar_target(
    name = config,
    command = util_get_config()
  )
)