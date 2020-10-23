#Hyperparameter training for model

#Source
source('RFiles/Source/tsCV_revised.R')

#Library
library(doMC)
library(forecast)
library(dplyr)

load("RFiles/Data/total_energy_coal_data.RData")

#load file if exists
destfile <- 'RFiles/Data/hyperparameters.RData'
if(file.exists(destfile)){
  load(destfile)
}
if(!file.exists(destfile)){
  #empty metric df
  model_metrics_arima_df <- data.frame()
}

#forecasting function for tsCV
forecast_fun <- function(px, x, h, xr, p_sub, d_sub, q_sub, ps_sub, ds_sub, qs_sub, l_sub, dr_sub){
  if(l_sub == "NULL"){l_sub <- NULL}
  lsub <- ifelse(!is.null(l_sub) & l_sub != "auto", as.numeric(l_sub), "auto")
forecast(
  Arima(
    x, 
    order=c(p_sub,d_sub,q_sub),
    seasonal=c(ps_sub,ds_sub,qs_sub),
    lambda = l_sub,
    include.drift = dr_sub,
    xreg=xr), 
  h=h, xreg=px)}

tst_e <- my_tsCV_vectorized(
  y = eng_coal_ts, 
  forecastfunction = forecast_fun,
  h = 4,
  window = 24,
  p_sub = 1,
  d_sub = 0,
  q_sub = 0,
  ps_sub = 1,
  ds_sub = 0,
  qs_sub = 0,
  l_sub = "NULL",
  dr_sub = FALSE,
  xreg=p_xreg)
  
data.frame(tst_e)

registerDoMC(3)
for(dr in c(FALSE, TRUE)){
  for(w in c('NULL', 12, 24)){
    if(w == "NULL"){w <- NULL}
    if(!is.null(w)){w <- as.numeric(w)}
    for(l in c('NULL', 'auto', 0)){
      for(ps in seq(0, 2, 1)){
        for(ds in seq(0, 2, 1)){
          for(qs in seq(0, 2, 1)){
            for(p in seq(0, 2, 1)){
              for(d in seq(0, 2, 1)){
                e_df <- foreach(q = seq(0,2,1), .combine = rbind) %dopar% {
                  tmp_e <-  my_tsCV_vectorized(
                    y = eng_coal_ts, 
                    forecastfunction = forecast_fun,
                    h = 24,
                    window = w,
                    p_sub = p,
                    d_sub = d,
                    q_sub = q,
                    ps_sub = ps,
                    ds_sub = ds,
                    qs_sub = qs,
                    l_sub = l,
                    dr_sub = dr,
                    xreg=p_xreg)
                  data.frame(
                    ModelName=paste('arima',"dr", dr, "w", w, "l", l,
                                    "ps", ps, "ds", ds, "qs", qs, 
                                    "p", p, "d", d, "q", q, sep='_'),
                    RMSE_12=sqrt(mean(tmp_e[,12]^2, na.rm=TRUE)),
                    RMSE_24=sqrt(mean(tmp_e[,24]^2, na.rm=TRUE)),
                    RunTime=Sys.time())
                }
                model_metrics_arima_df <- rbind(model_metrics_arima_df, e_df)
                print('latest model')
                print(data.frame(e_df) %>% arrange(RMSE_12))
                print('all models')
                print(model_metrics_arima_df %>% arrange(RMSE_12) %>% head(10))
                flush.console()
              }
              save(model_metrics_arima_df, file = destfile)
            }
          }
        }
      }
    }
  }
}

