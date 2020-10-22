---
title: "Modifying forecast's tsCV"
author: "John Grando"
date: "October 21, 2020"
output: 
  html_document:
    keep_md: true
---

Recently, I have found a use tsCV from the forecast package.  However, when using the `xreg` option I encountered a few issues and decided to look into them.  This report summarizes the findings and proposed fixes for this function.  First, a great thank you to Rob J Hyndman and George Athanasopoulos for providing such a wonderful resource that is freely available.




```r
library(forecast)
```

```
## Registered S3 method overwritten by 'quantmod':
##   method            from
##   as.zoo.data.frame zoo
```

For the practice data set, I have loaded a time series object and xreg matrix.  For more details on the subject matter, please refer to ().  For these purposes, it is only necessary to note that two predictor variables are provided in the `p_xreg` object.

If we try to use the tsCV function out of the box using xregs, we get an output of `NA` values


```r
load("total_energy_coal_data.RData")
my_fun <- function(x, h){forecast(Arima(x, order=c(2,0,0)), h=h, xreg=xreg)}
(e <- tsCV(eng_coal_ts, my_fun, h=1, xreg=p_xreg))
```

```
##      Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec
## 2010      NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2011  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2012  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2013  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2014  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2015  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2016  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2017  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2018  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2019  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA  NA
## 2020  NA  NA  NA  NA  NA  NA
```

To debug this problem, we can step through the function.  First, we will set the initial parameters


```r
y <- eng_coal_ts
h = 2
xreg = p_xreg
initial = 0
window = NULL
forecastfunction = my_fun
```




```r
y <- as.ts(y)
n <- length(y)
e <- ts(matrix(NA_real_, nrow = n, ncol = h))
if(initial >= n) stop("initial period too long")
tsp(e) <- tsp(y)
if (!is.null(xreg)) {
  # Make xreg a ts object to allow easy subsetting later
  xreg <- ts(as.matrix(xreg))
  if(NROW(xreg) != length(y))
    stop("xreg must be of the same size as y")
  tsp(xreg) <- tsp(y)
}
if (is.null(window)) {
  indx <- seq(1+initial, n - 1L)
} else {
  indx <- seq(window+initial, n - 1L, by = 1L)
}
```

Eventually we get to a for loop that contains the error.  Here we have set the looped value arbitrarily to 20.


```r
i = 20
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
  } else {
    xreg_subset <- as.matrix(subset(
      xreg,
      start = ifelse(is.null(window), 1L,
              ifelse(i - window >= 0L, i - window + 1L, stop("small window")))
    ))
    # The `...` has been removed and xreg has been inserted into the arguments
    # for out-of-function operation.
    fc <- forecastfunction(y_subset, h = h, xreg = xreg_subset)
  }
```

```
## Error in forecastfunction(y_subset, h = h, xreg = xreg_subset): unused argument (xreg = xreg_subset)
```
The error is not immediately obvious.  If we want to use `xreg` in the training, then we also need to specify an `xreg` in the prediction forecast which is the same length as `h`.  Therefore, we need to identify a new parameter to identify the prediction `xreg` argument.

As we debug this operation, there are a few other issues that arise:
1. The `subset()` function that creates `xreg_subset` does not have an end, thus returning a matrix with [i:length(xreg)].  This can be fixed by  adding an `end=i` term. 
2.  The subset does not behave well with multidimensional arrays because when they are subset, they are converted to 1 dimensional arrays.  It is much easier to slice the matrix and reformat it to the original 2D form.

3.  If we want to get maximum results for each prediction of `h` for each index location, a different function should be fit for each `h`.


```r
#interval from y_subset
#start = ifelse(is.null(window), 1L,
#        ifelse(i - window >= 0L, i - window + 1L, stop("small window")))
#prediction_xreg = xreg[start:h,]

prediction_xreg <- as.matrix(subset(
      xreg,
      start = ifelse(is.null(window), 1L,
              ifelse(i - window >= 0L, i - window + 1L, stop("small window"))),
      end = h
    ))

    xreg_subset <- as.matrix(subset(
      xreg,
      start = ifelse(is.null(window), 1L,
              ifelse(i - window >= 0L, i - window + 1L, stop("small window"))),
      end = i
    ))

my_fun <- function(x, h, px, xr){forecast(Arima(x, order=c(2,0,0), xreg=xr), h=h, xreg=px)}
forecastfunction <- my_fun
fc <- forecastfunction(x = y_subset, h = h, px=prediction_xreg, xr=xreg_subset)
if (!is.element("try-error", class(fc))) {
    e[i, ] <- y[i + (1:h)] - fc$mean
}
print(e[20,])
```

```
##   Series 1   Series 2 
##  -7295.528 -24637.935
```
The changes above, as well as a few formatting fixes (which may be due to an updated R version), have been applied to the function below


```r
tsCV_v2 <- function(y, forecastfunction, h=1, window=NULL, xreg=NULL, initial=0, console_print=NULL, ...) {
  y <- as.ts(y)
  n <- length(y)
  e <- ts(matrix(NA_real_, nrow = n, ncol = h))
  if(initial >= n) stop("initial period too long")
  tsp(e) <- tsp(y)
  if (!is.null(xreg)) {
    # Make xreg a ts object to allow easy subsetting later
    #### Removed ts() since using subset() on matrices is difficut (multiple xreg)
    xreg <- as.matrix(xreg)
    if(NROW(xreg) != length(y))
      stop("xreg must be of the same size as y")
    tsp(xreg) <- tsp(y)
  }
  #MISSING BRACKETS CAUSING ERROR, CHECK FOR WINDOW LATER
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
    } else { #SYNTACTIC ISSUE  MOVE ONE LINE ABOVE
      start = ifelse(is.null(window), 1L,
              ifelse(i - window >= 0L, i - window + 1L, stop("small window")))
      ####
      end = i
      ####
      xreg_subset <- matrix(xreg[start:end,], ncol = ncol(xreg))
      #process h differently due to errors that can happen near the end of the index
      #xreg_prediction <- try(matrix(xreg[i:(i+h-1),], ncol = ncol(xreg)), silent = TRUE)
      make_prediction_m <- function(h, i) {try(matrix(xreg[i:(i+h-1),], ncol = ncol(xreg)), silent = TRUE)}
      prediction_l <- lapply(1:h, make_prediction_m, i=i)
      fc_fun <- function(px, y_subset, xr, ...) {
          try(suppressWarnings(
            forecastfunction(y_subset, h = nrow(px), xr = xreg_subset, px = px, ...)
          ), silent = TRUE)
      }
      fc_l <- lapply(prediction_l, fc_fun, y_subset=y_subset, xr=xreg_subset)
      f_mean_l <- lapply(fc_l, function(x){if (!is.element("try-error", class(x))){x$mean}})
      max_h <- which.max(lapply(f_mean_l, length))
      f_mean <- f_mean_l[[max_h]]
    }
    #if (!is.element("try-error", class(fc)) & !(is.element("try-error", class(xreg_prediction)))) {
    if (!is.null(f_mean)) {
      tmp_result <- as.vector(y[i + (1:max_h)] - f_mean)
      result <- c(
        tmp_result, 
        rep(NA,h-length(tmp_result)))
      e[i, ] <- result
      #if(!is.null(console_print)){
      #  pe[i, ] <- 100 * (y[i + (1:h)] - fc$mean[1:h]) / y[i + (1:h)] 
      #if (i %% 100 == 0){cat(paste(i,"\n", sep = ' '))}
      #cat(".")
      #}
    }
  }
  if (h == 1) {
    return(e[, 1L])
  } else {
    colnames(e) <- paste("h=", 1:h, sep = "")
    return(e)
  }
}
```

Additionally, the new tsCV function has been vectorized, which removes all uses of for loops.  This should provide better performance for larger data sets.


```r
tsCV_v2_vectorized <- function(y, forecastfunction, h=1, window=NULL, xreg=NULL, initial=0, ...) {
  y <- as.ts(y)
  n <- length(y)
  e <- ts(matrix(NA_real_, nrow = n, ncol = h))
  if(initial >= n) stop("initial period too long")
  tsp(e) <- tsp(y)
  if (!is.null(xreg)) {
    # Make xreg a ts object to allow easy subsetting later
    #### Removed ts() since using subset() on matrices is difficut (multiple xreg)
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
      ####
      x_end = i
      ####
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
    #run function and return list of xreg predictors per indx
    fc_fun <- function(i, y_subset_l, prediction_l, xreg_subset_l, ...) {
      prediction <- prediction_l[[i]]
      y_subset <- y_subset_l[[i]]
      xr = xreg_subset_l[[i]]
      lapply(
        prediction[1:h],
        function(px){
          try(suppressWarnings(
            forecastfunction(px = px, x = y_subset, h = nrow(px), xr = xr, ...)
          ), silent = TRUE)
        })
    }
    #From this point forward, process the lists using their order, which is different
    #than the raw indx due to window and initial inputs
    fc_l <- lapply(1:length(indx), fc_fun, y_subset_l=y_subset_l, prediction_l=prediction_l, xreg_subset_l=xreg_subset_l)
    #extract means from each predictor subset of indx
    f_mean_long_l <- lapply(
        fc_l, 
        function(x){
          lapply(
            1:h,
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
    f_tmp <- c(
      f_mean_l[[i]],
      rep(NA,h-length(f_mean_l[[i]]))
    )
    tmp_result <- as.vector(y[i + (1:max_h)] - f_tmp)
    result <- c(
      tmp_result, 
      rep(NA,h-length(tmp_result)))
  }
  errors_l <- lapply(1:length(indx), errors_fun)
  window <- ifelse(is.null(window),0,window)
  lapply(1:length(indx), function(x){e[x+window+initial,] <<- errors_l[[x]]})
  if (h == 1) {
    return(e[, 1L])
  } else {
    colnames(e) <- paste("h=", 1:h, sep = "")
    return(e)
  }
}
```

Test using no `xreg`.  This allows the original function to be compared to the two new options


```r
my_fun <- function(x, h){forecast(Arima(x, order=c(2,0,0)), h=h)}

e1_start <- proc.time()
e1 <- tsCV(eng_coal_ts, my_fun, h=3)
original_function_duration <- proc.time() - e1_start
e2_start <- proc.time()
e2 <- tsCV_v2(eng_coal_ts, my_fun, h=3)
fixed_duration <- proc.time() - e2_start
e3_start <- proc.time()
e3 <- tsCV_v2_vectorized(eng_coal_ts, my_fun, h=3)
fixed_vector_duration <- proc.time() - e3_start
all.equal(e1,e2, e2, tolerance=0.1)
```

```
## [1] TRUE
```

```r
original_function_duration
```

```
##    user  system elapsed 
##   1.672   0.012   1.685
```

```r
fixed_duration
```

```
##    user  system elapsed 
##   1.603   0.000   1.604
```

```r
fixed_vector_duration
```

```
##    user  system elapsed 
##   1.607   0.004   1.611
```

Test using `xreg`.  Note, the original function could not be compared, as it provides erroneous output


```r
my_fun <- function(px, x, h, xr){forecast(Arima(x, order=c(2,0,0), xreg=xr), h=h, xreg=px)}
e2xr_start <- proc.time()
e2xr <- tsCV_v2(eng_coal_ts, my_fun, h=3, xreg=p_xreg)
fixed_duration <- proc.time() - e2xr_start
e3xr_start <- proc.time()
e3xr <- tsCV_v2_vectorized(eng_coal_ts, my_fun, h=3, xreg=p_xreg)
fixed_vector_duration <- proc.time() - e3xr_start
all.equal(e2xr, e2xr, tolerance=0.1)
```

```
## [1] TRUE
```

```r
fixed_duration
```

```
##    user  system elapsed 
##   9.411   0.004   9.417
```

```r
fixed_vector_duration
```

```
##    user  system elapsed 
##  10.005   0.000  10.007
```
