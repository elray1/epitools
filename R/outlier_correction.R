#' Correct and optionally redistribute outliers in an `epi_tibble` object.
#'
#' Outliers are replaced with a corrected value. Optionally, the difference
#' between the initial outlying value and the corrected value may be
#' redistributed to other times.
#'
#' @param x The `epi_tibble` object under consideration.
#' @param var The variable in `x` in which to correct outliers.
#' @param outliers_col The name of a variable in `x` containing information
#'   about outliers (detection thresholds and replacement values), in the format
#'   as returned by `detect_outliers`. The default value is "outliers".
#' @param distribution_method A string specifying how the difference between the
#'   original value and the corrected value should be redistributed to other
#'   times. This may be `"none"` to do no redistribution, `"prop"` to
#'   redistribute proportionally to existing values (a larger amount of the
#'   difference is distributed to dates where the signal has larger values),
#'   `"zeros"` to redistribute to dates where the signal has a value of 0, or
#'   `"equal"` to redistribute approximately equally across a range of dates.
#' @param distribution_start the earliest index value to which we may
#'   redistribute a difference between the initial value and corrected value.
#'   If this column is not provided or the value is missing in a given row, a
#'   default of the earliest available index value will be used.
#' @param distribution_end the last index value to which we may
#'   redistribute a difference between the initial value and corrected value.
#'   If this column is not provided or the value is missing in a given row, a
#'   default of the `time_value` of the outlier minus one time step will be
#'   used.
#' @param outliers A tibble specifying outliers and corrections to make; if
#'   this argument is provided, the `outliers_col`, `detection_method`,
#'   `distribute_strategy`, `distribute_start`, and `distribute_end` arguments
#'   are all ignored.
#'   This argument allows for a more flexible per-outlier strategy for outlier
#'   correction and redistribution by supplying the following columns:
#' * `geo_value`: the geographic location
#' * `[optional additional key columns]`: if more variables than the `geo_value`
#'   are requierd to identify a unique forecast, they must be included. This
#'   should minimally include any grouping variables on the input `epi_tibble`
#'  `x`.
#' * `time_value`: the date or time of the outlier to be corrected
#' * `replacement` <num>: the corrected value that will replace the outlier
#' * `distribution_method` <str>: optional, as documented above
#' * `distribution_start` <date>: optional, as documented above
#' * `distribution_end` <date>: optional, as documented above
#' @param new_col_name String indicating the name of the new column that will
#'   contain the results of outlier correction. Default is the value of the
#'   supplied `var` argument concatenated with `_corrected`. Note that
#'   setting `new_col_name` equal to an existing column name will overwrite this
#'   column.
#'
#' @return a new `epi_tibble` with corrected outliers
#'
#' @importFrom dplyr group_modify mutate
#' @importFrom rlang abort enquo
#' @export
correct_outliers = function(x, var,
                            outliers_col = "outliers",
                            detection_method = NULL,
                            distribution_method = c("none"),
                            distribution_start = min(x$time_value),
                            distribution_end = max(x$time_value),
                            outliers = NULL,
                            new_col_name = "corrected_value") {
  # Used below to validate distribution_method column in `outliers` argument
  # update if the list of valid methods expands!
  VALID_METHODS = c("none")

  # Check that we have a variable to do computations on
  if (missing(var)) abort("`var` must be specified.")
  var = enquo(var)

  # Validate distribution method
  distribution_method = match.arg(distribution_method)

  # Validate distribution_start and distribution_end properly ordered
  if (distribution_start > distribution_end) {
    stop("distribution_start can't be after distribution_end.")
  }

  # Extract grouping variables for x
  grouping_vars = group_vars(x)

  # Expected columns for the outliers data frame
  expected_outliers_cols = unique(c(grouping_vars, "geo_value", "time_value",
                                  "replacement", "distribution_method",
                                  "distribution_start", "distribution_end"))

  # Ensure that there are not duplicate time_values within one group of x
  time_value_counts <- x %>% count(time_value) %>% pull(n) %>% unique()
  if (!identical(time_value_counts, 1L)) {
    stop("x must not have duplicate values of `time_value` within grouping ",
         "levels. Do you need to add more grouping structure?")
  }

  # Extract outliers tibble if not provided, else validate provided tibble
  if (is.null(outliers)) {
    # grab just the rows identified as outliers, then append columns describing
    # how to do redistribution
    outliers = x %>%
      slice_outliers(
        var = var,
        outliers_col = outliers_col,
        detection_method = detection_method) %>%
      mutate(
        distribution_method = distribution_method,
        distribution_start = distribution_start,
        distribution_end = distribution_end) %>%
      select(all_of(expected_outliers_cols))
  } else {
    # has correct columns
    if (!is.data.frame(outliers) ||
        !all(sort(expected_outliers_cols) == sort(colnames(outliers)))) {
      stop("outliers argument must be a data frame with columns `geo_value`, ",
           "`time_value`, `replacement`, `distribution_method`, ",
           "`distribution_start`, and `distribution_end` in addition to any ",
           "grouping variables for the input `x`.")
    }

    # add distribute_strategy column if not provided, else validate provided column
    if (!("distribution_method" %in% colnames(outliers))) {
      outliers$distribution_method = distribution_method
    } else if (!all(outliers$distribution_method %in% VALID_METHODS)) {
      stop(paste0("distribution_method must be one of ",
           paste0(VALID_METHODS, collapse = " ")))
    }

    # add distribute_start column if not provided
    if (!("distribute_start" %in% colnames(outliers))) {
      outliers$distribute_start = distribute_start
    }

    # add distribute_end column if not provided
    if (!("distribute_end" %in% colnames(outliers))) {
      outliers$distribute_end = distribute_end
    }

    # Validate distribute_start and distribute_end properly ordered
    if (any(outliers$distribute_start > outliers$distribute_end)) {
      stop("distribute_start can't be after distribute_end.")
    }
  }

  # Save the metadata (dplyr drops it)
  metadata = attributes(x)$metadata

  # Do outlier correction for each group in x
  # Need to pass .keep = TRUE in case grouping variables include geo_value,
  # since downstream methods may call epitools functions expecting that
  # column to be present.
  # However, group_modify requires that the result not include grouping
  # variables; we extract the names of those variables and pass them to
  # correct_outliers_one_grp so that that function can drop them.
  # Is there a better way around this?
  x = x %>%
    group_modify(correct_outliers_one_grp,
                 var = var,
                 outliers = outliers,
                 new_col_name = new_col_name,
                 grouping_vars = grouping_vars,
                 .keep = TRUE)

  # Attach the class and metadata and return
  class(x) = c("epi_tibble", class(x))
  attributes(x)$metadata = metadata
  return(x)
}



#' Keep rows identified as outliers by a specified detection method
#' 
#' @importFrom tidyr unnest pivot_longer
#' @importFrom dplyr filter
slice_outliers = function(x, var, outliers_col, detection_method) {
  # Check that we have a variable to do computations on
  # if (missing(var)) abort("`var` must be specified.")
  # var = enquo(var)
  # outliers_col = enquo(outliers_col)
  # detection_method = enquo(detection_method)

  x %>%
    unnest(!!outliers_col) %>%
    pivot_longer(
      cols = x %>% pull(!!outliers_col) %>% `[[`(1) %>% colnames(),
      names_to = c("detection_method", ".value"),
      names_pattern = "(.+)_(.+)"
    ) %>%
    filter(
      detection_method == !!detection_method,
      (!!var < lower) | (!!var > upper)
    )
}



#' Function to do outlier correction for one group.
#'
#' This function is not intended to be called by package users.
#'
#' @param .data_group epi_tibble for one group. should represent one combination
#'   of key factors determining a reporting unit, e.g., one geo_type or one
#'   combination of geo_type and age group
#' @param grouping_vars variables that defined groups in the original input to
#'   correct_outliers. These will be dropped from the return value for this
#'   function
#' @inheritParams detect_outliers
#'
#' @importFrom dplyr select mutate
#' @importFrom purrr map pmap_dfc
#' @importFrom tidyselect ends_with all_of
#' @importFrom tibble as_tibble
#' @importFrom rlang abort enquo
correct_outliers_one_grp = function(.data_group,
                                    var,
                                    outliers,
                                    new_col_name,
                                    grouping_vars,
                                    ...) {
  result = .data_group %>% pull(!!var)

  # just keep outliers for the current group (or, minimally, geo_value)
  key_vars = unique(c(grouping_vars, "geo_value"))
  grp_outliers = .data_group %>%
    select(all_of(key_vars)) %>%
    distinct() %>%
    left_join(outliers, by = key_vars)

  # Get the row index of each outlier in the .data_group object
  join_cols = c(key_vars, "time_value")
  row_indices = grp_outliers %>%
    left_join(.data_group %>% mutate(outlier_row_index = seq_len(n())),
              by = join_cols) %>%
    pull(outlier_row_index)

  # throw a warning if row indices were not identified for some outliers
  # (user tried to correct outliers for a key/time_value pair not in the data)
  if (any(is.na(row_indices))) {
    warning("Some provided outliers did not match rows in the data: ",
            paste(
              purrr::map_chr(which(is.na(row_indices)),
                            function(i) {
                              paste(join_cols,
                                    grp_outliers[i, join_cols],
                                    sep = ": ", collapse = ", ")
                            }),
              collapse = "; "))
    grp_outliers = grp_outliers[!is.na(row_indices), ]
    row_indices = row_indices[!is.na(row_indices)]
  }

  # Correct outliers sequentially in order they were provided
  # (typically, starting from the beginning of the signal)
  for (i in seq_len(nrow(grp_outliers))) {
    # information about where the outlier is and replacement value
    row_index = row_indices[i]
    cur_value = result[row_index]
    replacement = grp_outliers$replacement[i]
    resid = cur_value - replacement

    # do replacement
    result[row_index] = replacement

    # redistribute residual
    distribution_method = grp_outliers$distribution_method[i]
    if (distribution_method != "none") {
      # assemble list of arguments to method
      method_args = list("x" = .data_group, "var" = var,
        "resid" = resid,
        "distribution_start" = grp_outliers$distribution_start[i],
        "distribution_end" = grp_outliers$distribution_end[i]
      )
      # TODO: add in other method args
      # method_args <- c(method_args, )
      # call the method
      result = do.call(method, args = method_args)
    }
  }

  # return
  return(.data_group %>%
           select(-all_of(grouping_vars)) %>%
           mutate(!!new_col_name := result))
}
