#Hyperparameter training for model

#Source
source('RFiles/local_cv_test.R')

#Library
library(doMC)
library(forecast)
library(dplyr)

load("RFiles/total_energy_coal_data.RData")

#load file if exists
destfile <- 'RFiles/hyperparameters.RData'
if(file.exists(destfile)){
  load(destfile)
}
if(!file.exists(destfile)){
  #empty metric df
  model_metrics_arima_df <- data.frame()
}

#forecasting function for tsCV
forecast_fun <- function(y, h = h, xreg=NULL, lambda_sub=NULL, p_sub=NULL, d_sub=NULL, q_sub=NULL, drift_sub=drift) {
  if(lambda_sub=='NULL'){
    lambda_sub=NULL
  }
  tmp_model <- Arima(y, 
                     xreg = p_xreg,
                     lambda = lambda_sub,
                     include.drift = drift_sub,
                     order=c(p_sub, d_sub, q_sub))
  tmp_fit <- tmp_model %>% 
    forecast(h=h, xreg=xreg[(length(xreg[,1])-h):length(xreg[,1]),])
  return(tmp_fit)
}

registerDoMC(3)
for (dr in c(FALSE, TRUE)){
  for (l in c('NULL', 'auto', 0)){
    for(p in seq(0, 2, 1)){
      for(d in seq(0, 2, 1)){
        tmp_df <- foreach(q = seq(0,2,1), .combine = rbind) %dopar% {
          tmp_csv <- tsCV_jg(y = eng_coal_ts, 
                             forecastfunction = forecast_fun,
                             h=12,
                             initial=24,
                             window=12,
                             extra_args = TRUE,
                             lambda_sub=l,
                             p_sub=p,
                             d_sub=d,
                             q_sub=q,
                             drift=dr)
          data.frame(
            ModelName=paste('arima', 'l', as.character(l), 'p', p, 'd', d, 'q', q, 'dr', dr, sep='_'),
            RMSE_10=sqrt(mean(tmp_csv$e[,10]^2, na.rm=TRUE)),
            MAPE_10=mean(abs(tmp_csv$pe[,10]), na.rm = TRUE),
            RunTime=Sys.time())
        }
        model_metrics_arima_df <- rbind(model_metrics_arima_df, tmp_df)
        print('latest model')
        print(tmp_df %>% arrange(RMSE_10))
        print('all models')
        print(model_metrics_arima_df %>% arrange(RMSE_10) %>% head(10))
        flush.console()
      }
    }
  }
}
save(model_metrics_arima_df, file = destfile)
