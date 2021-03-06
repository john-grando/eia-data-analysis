#Hyperparameter training for model

#Source
source('./Source/tsCV_revised.R')

#Library
library(doMC)
library(forecast)
library(dplyr)

load("./Data/total_energy_coal_data.RData")

#load file if exists
destfile <- './Data/hyperparameters.RData'
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

tst_e <- my_tsCV(
  y = eng_coal_train_ts, 
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
  xreg=p_xreg_train)
  
data.frame(tst_e)

registerDoMC(3)
for(w in c('NULL', 60)){
  if(w == "NULL"){w <- NULL}
  if(!is.null(w)){w <- as.numeric(w)}
  for(l in c('NULL', 'auto', 0)){
    for(dr in c(FALSE, TRUE)){
      for(ps in seq(0, 2, 1)){
        for(ds in seq(0, 2, 1)){
          for(qs in seq(0, 2, 1)){
            for(p in seq(0, 2, 1)){
              for(d in seq(0, 2, 1)){
                #do not allow modeling with drift and d > 1
                if(d > 1 & dr == TRUE){next}
                e_df <- foreach(q = seq(0,2,1), .combine = rbind) %dopar% {
                  tmp_e <-  my_tsCV_vectorized(
                    y = eng_coal_train_ts, 
                    forecastfunction = forecast_fun,
                    h = 24,
                    window = w,
                    shortened = TRUE,
                    p_sub = p,
                    d_sub = d,
                    q_sub = q,
                    ps_sub = ps,
                    ds_sub = ds,
                    qs_sub = qs,
                    l_sub = l,
                    dr_sub = dr,
                    xreg=p_xreg_train)
                  data.frame(tmp_e) %>% 
                    group_by() %>% 
                    summarize_all(
                      list(
                        mae = ~ mean(abs(.), na.rm=TRUE), 
                        rmse = ~mean(.^2, na.rm = TRUE))) %>% 
                    mutate(
                      model_name = paste(
                        'arima',"dr", dr, "w", w, "l", l,
                        "ps", ps, "ds", ds, "qs", qs, 
                        "p", p, "d", d, "q", q, sep='_'),
                      run_time = Sys.time())
                }
                model_metrics_arima_df <- rbind(model_metrics_arima_df, e_df)
                print('latest model')
                print(data.frame(e_df) %>%  
                  select(model_name, run_time, h.12_mae, h.24_mae, h.12_rmse, h.24_rmse) %>% 
                  arrange(h.24_rmse))
                print('all models')
                print(model_metrics_arima_df %>% 
                  select(model_name, run_time, h.12_mae, h.24_mae, h.12_rmse, h.24_rmse) %>% 
                  arrange(h.24_rmse) %>% 
                  head(10))
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

