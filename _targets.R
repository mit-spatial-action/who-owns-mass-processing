# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
# library(tarchetypes) # Load other packages as needed.

# Set target options:
targets::tar_option_set(
  packages = c("tibble"), # Packages that your targets need for their tasks.
  format = "qs"
)

targets::tar_source()

config <- config::get()

list(
  targets::tar_target(
    name = data,
    command = tibble(x = rnorm(100), y = rnorm(100))
    # format = "qs" # Efficient storage for general data objects.
  ),
  targets::tar_target(
    name = model,
    command = coefficients(lm(y ~ x, data = data))
  )
)
