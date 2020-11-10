# Analysis and visualization of US Energy Information

## Source Data  
- plant data location (source - U.S. Energy Information Administration): https://www.eia.gov/opendata/bulkfiles.php  
- weather data location (source - NOAA): https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/


## PreProcessed Data  From EIA Bulk Downloads  
- Total Energy (app/PreProcess/total_energy.py)  
  - Processed/TotalEnergyFactDF - Facts  
  - Processed/TotalEnergyDimDF - Dimensions  
- Electricity (app/PreProcess/electricity.py)  
  - Processed/ElectricityFactDF - Facts
  - Processed/ElectricityPowerDimDF - Power generation/consumption
  - Processed/ElectricityPlantLevelDimDF - Individual plant
  - Processed/ElectricityRetailDimDF - sales information
  - Processed/ElectricityFossilFuelDimDF - Fuel type information including cost
  - Processed/ElectricityFossilFuelQualityDimDF - Fuel type quality
