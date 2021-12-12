"""
Title: flagging_report.py

Goal: The workflow code used to create the flagging reports every two weeks.
"""

import pandas as pd
import shapefile as shf
import matplotlib.pyplot as plt
import numpy as np

#   read in the project shapefile
sf = shf.Reader(r"D:\sample_biodiversitree\Shapefiles\plots_2015.shp") # -- info about the shapefiles

#   read in the plot logger and sensor information
plot_design = pd.read_csv(r"D:\sample_biodiversitree\table_design\plot_copy.csv") # -- plot design
plot_design = plot_design[['plot', 'logger']]

#   read in the most recent error information and keep only relevant info
air_error = pd.read_csv(r"D:\sample_biodiversitree\data\error_reports\error_report_air.csv")
air_error = air_error[['plot', 'error_type']]

soil_error = pd.read_csv(r"D:\sample_biodiversitree\data\error_reports\error_report_soil.csv")
soil_error = soil_error[['plot', 'error_type']]

#   merge the error files with their plot design information
air_err = pd.concat([air_error, plot_design], axis=1)
soil_err = pd.concat([soil_error, plot_design], axis=1)

#   group the merged table by the plot numbers and the error types
grp_plt_err = air_error.groupby(by=['plot', 'error_type']).count().index.get_level_values('plot').tolist()

#error_plots = grp_plt_err['plot'] #   isolate the plot numbers as list

#   function to turn shapefile into dataframe
def read_shapefile(sf):
    """
    Read a shapefile into a Pandas dataframe with a 'coords' 
    column holding the geometry information. This uses the pyshp
    package
    """
    fields = [x[0] for x in sf.fields][1:]
    records = sf.records()
    shps = [s.points for s in sf.shapes()]    
    df = pd.DataFrame(columns=fields, data=records)
    df = df.assign(coords=shps)    
    return df

#   function for filling the maps with color
def plot_map_fill_multiples_ids(title, plot_list, sf, 
                                               x_lim = None, 
                                               y_lim = None, 
                                               figsize = (11,9), 
                                               color = 'r'):
    '''
    Plot map with lim coordinates
    '''
    
    plt.figure(figsize = figsize)
    fig, ax = plt.subplots(figsize = figsize)
    fig.suptitle(title, fontsize=16)    
    for shape in sf.shapeRecords():
        x = [i[0] for i in shape.shape.points[:]]
        y = [i[1] for i in shape.shape.points[:]]
        ax.plot(x, y, 'k')
            
    for id in plot_list:
        shape_ex = sf.shape(id)
        x_lon = np.zeros((len(shape_ex.points),1))
        y_lat = np.zeros((len(shape_ex.points),1))
        for ip in range(len(shape_ex.points)):
            x_lon[ip] = shape_ex.points[ip][0]
            y_lat[ip] = shape_ex.points[ip][1]
        ax.fill(x_lon,y_lat, color)
             
        x0 = np.mean(x_lon)
        y0 = np.mean(y_lat)
        plt.text(x0, y0, sf.records()[id][2], fontsize=10)
    
    if (x_lim != None) & (y_lim != None):     
        plt.xlim(x_lim)
        plt.ylim(y_lim)


#   function that plots error map based upon the plot number
def plot_error_map(sf, title, plots, color):
    '''
    Plot map with selected plot, using specific color
    '''
    
    df = read_shapefile(sf)
    plot_id = []
    for i in plots:
        plot_id.append(df[df['plot_12'] == i]
                         .index.values[0])
    plot_map_fill_multiples_ids(title, plot_id, sf, 
                                       x_lim = None, 
                                       y_lim = None, 
                                       figsize = (11,9), 
                                       color = color)

#   create error map graphic

plot_error_map(sf, 'Error Map: Date (make this current date)', grp_plt_err, 'r')
plt.show()

#   create error table -> logger|plot|error_type|error_count
#   create pdf report document
