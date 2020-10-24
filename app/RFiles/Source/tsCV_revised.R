
# for xreg to work, the prediction xreg must be named px
# because of how the forecast function is run, the same number of predictors in xreg have to be supplied as h; therefore,
# one run for each h is performed.  If you are okay with losing some predictions at the rows (indx), then you can pass
# 'shortened = TRUE' and just one prediction at h will be run, but you will lose and partial 1:(h-1) prediction values
# at that indx row.

my_tsCV <- function(y, forecastfunction, h=1, window=NULL, xreg=NULL, initial=0, console_print=NULL, ...) {
  y <- as.ts(y)
  n <- length(y)
  e <- ts(matrix(NA_real_, nrow = n, ncol = h))
  if(initial >= n) stop("initial period too long")
  tsp(e) <- tsp(y)
  if (!is.null(xreg)) {
    xreg <- as.matrix(xreg)
    if(NROW(xreg) != length(y))
      stop("xreg must be of the same size as y")
    tsp(xreg) <- tsp(y)
  }
  if (is.null(window)) {
    indx <- seq(1+initial, n - 1L)
  } else {
    indx <- seq(window+initial, n - 1L, by = 1L)
  }
  for (i in indx) {
    y_subset <- subset(
      y,
      start = ifelse(is.null(window), 1L,
                     ifelse(i - window >= 0L, i - window + 1L, stop("small window"))
      ),
      end = i
    )
    if (is.null(xreg)) {
      fc <- try(suppressWarnings(
        forecastfunction(y_subset, h = h, ...)
      ), silent = TRUE)
      f_mean <- if (!is.element("try-error", class(fc))){fc$mean}
      max_h <- h
    } else {
      start = ifelse(is.null(window), 1L,
                     ifelse(i - window >= 0L, i - window + 1L, stop("small window")))
      end = i
      xreg_subset <- matrix(xreg[start:end,], ncol = ncol(xreg))
      #process each h separately due to errors that can happen near the end of the index
      make_prediction_m <- function(h, i) {try(matrix(xreg[i:(i+h-1),], ncol = ncol(xreg)), silent = TRUE)}
      prediction_l <- lapply(1:h, make_prediction_m, i=i)
      fc_fun <- function(px, y_subset, xr, ...) {
        try(suppressWarnings(
          forecastfunction(y_subset, h = nrow(px), xr = xreg_subset, px = px, ...)
        ), silent = TRUE)
      }
      fc_l <- lapply(prediction_l, fc_fun, y_subset=y_subset, xr=xreg_subset, ...)
      f_mean_l <- lapply(fc_l, function(x){if (!is.element("try-error", class(x))){x$mean}})
      max_h <- which.max(lapply(f_mean_l, length))
      f_mean <- f_mean_l[[max_h]]
    }
    if (!is.null(f_mean)) {
      tmp_result <- as.vector(y[i + (1:max_h)] - f_mean)
      result <- c(
        tmp_result, 
        rep(NA,h-length(tmp_result)))
      e[i, ] <- result
    }
  }
  if (h == 1) {
    return(e[, 1L])
  } else {
    colnames(e) <- paste("h=", 1:h, sep = "")
    return(e)
  }
}

my_tsCV_vectorized <- function(y, forecastfunction, h=1, window=NULL, xreg=NULL, initial=0, shortened=FALSE, ...) {
  y <- as.ts(y)
  n <- length(y)
  e <- ts(matrix(NA_real_, nrow = n, ncol = h))
  if(initial >= n) stop("initial period too long")
  tsp(e) <- tsp(y)
  if (!is.null(xreg)) {
    xreg <- as.matrix(xreg)
    if(NROW(xreg) != length(y))
      stop("xreg must be of the same size as y")
    tsp(xreg) <- tsp(y)
  }
  if (is.null(window)) {
    indx <- seq(1+initial, n - 1L)
  } else {
    indx <- seq(window+initial, n - 1L, by = 1L)
  }
  y_subset_fun <- function(i){
    subset(
      y,
      start = ifelse(is.null(window), 1L,
                     ifelse(i - window >= 0L, i - window + 1L, stop("small window"))
      ),
      end = i
    )
  }
  y_subset_l <- lapply(indx, y_subset_fun)
  if (is.null(xreg)) {
    fc_fun <- function(y_subset, h=h){try(suppressWarnings(
      forecastfunction(y_subset, h = h)
    ), silent = TRUE)}
    fc_l <- lapply(y_subset_l, fc_fun, h=h)
    f_mean_l <- lapply(fc_l, function(x){if (!is.element("try-error", class(x))){x$mean}})
    max_h <- h
  } else {
    xreg_subset_fun <- function(i){
      x_start = ifelse(is.null(window), 1L,
                       ifelse(i - window >= 0L, i - window + 1L, stop("small window")))
      x_end = i
      matrix(xreg[x_start:x_end,], ncol = ncol(xreg))
    }
    #subset xreg for each indx
    xreg_subset_l <- lapply(indx, xreg_subset_fun)
    #subset xreg predictor for each h per each indx
    prediction_per_h_subset <- function(i, h){
      lapply(
        1:h, 
        function(x, i){
          try(matrix(xreg[i:(i+x-1),], ncol = ncol(xreg)), silent = TRUE)
        }, 
        i=i)
    }
    #make list of xreg predictors per indx
    prediction_l <- lapply(indx, prediction_per_h_subset, h=h)
    #run function for each h for each indx and return h predictions per indx
    # if shortened = TRUE then only one prediction of length h is used.
    fc_fun <- function(i, y_subset_l, prediction_l, xreg_subset_l, ...) {
      prediction <- prediction_l[[i]]
      y_subset <- y_subset_l[[i]]
      xr = xreg_subset_l[[i]]
      h_start <- ifelse(shortened == TRUE, h, 1)
      lapply(
        prediction[h_start:h],
        function(px){
          try(suppressWarnings(
            forecastfunction(px = px, x = y_subset, h = nrow(px), xr = xr, ...)
          ), silent = TRUE)
        })
    }
    #From this point forward, process the lists using their order, which is different
    #than the raw indx due to window and initial inputs
    fc_l <- lapply(1:length(indx), fc_fun, y_subset_l=y_subset_l, prediction_l=prediction_l, xreg_subset_l=xreg_subset_l, ...)
    #extract values from each subset of h in each indx
    f_mean_long_l <- lapply(
      fc_l, 
      function(x){
        h_end <- ifelse(shortened==TRUE,1,h)
        lapply(
          1:h_end,
          function(x2){
            if (!is.element("try-error", class(x[[x2]]))){x[[x2]]$mean}}
        )
      })
    #find longest arrary of each subset and use that one for the indx
    max_l <- lapply(f_mean_long_l, function(x){which.max(lapply(x, length))})
    f_mean_l <- lapply(1:length(indx), function(x){f_mean_long_l[[x]][[max_l[[x]][[1]]]]})
    max_h <- h
  }
  #process errors
  errors_fun <- function(i){
    #indx[i] refers to the original row location
    f_tmp <- c(
      f_mean_l[[i]],
      rep(NA,h-length(f_mean_l[[i]]))
    )
    tmp_result <- as.vector(y[indx[i] + (1:max_h)] - f_tmp)
    e[indx[i], ] <<- c(
      tmp_result, 
      rep(NA,h-length(tmp_result)))
  }
  lapply(1:length(indx), errors_fun)
  if (h == 1) {
    return(e[, 1L])
  } else {
    colnames(e) <- paste("h=", 1:h, sep = "")
    return(e)
  }
}