#   A tester file for playing with files, folders, etc.

#   trying to figure these shape files teehee

import pandas as pd
import shapefile as shf
import matplotlib.pyplot as plt
import numpy as np


sf = shf.Reader(r"D:\sample_biodiversitree\Shapefiles\plots_2015.shp") # -- info about the shapefiles
"""
plot_design = pd.read_csv(r"D:\sample_biodiversitree\table_design\plot_copy.csv") # -- plot design

air_error = pd.read_csv(r"D:\sample_biodiversitree\data\error_reports\error_report_air.csv")
soil_error = pd.read_csv(r"D:\sample_biodiversitree\data\error_reports\error_report_soil.csv")

air_error = air_error[['plot', 'error_type']]
soil_error = soil_error[['plot', 'error_type']]
plot_design = plot_design[['plot', 'logger']]

air_err = pd.concat([air_error, plot_design], axis=1)
soil_err = pd.concat([soil_error, plot_design], axis=1)

#   merge the information from the error tables and the plot design table
air_error = air_error.merge(plot_design, how='left', on='plot')
soil_error = soil_error.merge(plot_design, how='left', on='plot')

#   group the merged table by the plot numbers and the error types
group_ex = air_error.groupby(by=['plot', 'error_type']).count()
"""

#   function to convert shapefile data into a dataframe format
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

shapes = read_shapefile(sf)
#   function to plot a singular shape in the files
def plot_shape(id, s=None):
    """ PLOTS A SINGLE SHAPE """
    plt.figure()
    ax = plt.axes()
    ax.set_aspect('equal')
    shape_ex = sf.shape(id)
    x_lon = np.zeros((len(shape_ex.points),1))
    y_lat = np.zeros((len(shape_ex.points),1))
    for ip in range(len(shape_ex.points)):
        x_lon[ip] = shape_ex.points[ip][0]
        y_lat[ip] = shape_ex.points[ip][1]    
        plt.plot(x_lon,y_lat) 
    x0 = np.mean(x_lon)
    y0 = np.mean(y_lat)
    plt.text(x0, y0, s, fontsize=10)
    # use bbox (bounding box) to set plot limits
    plt.xlim(shape_ex.bbox[0],shape_ex.bbox[2])
    return x0, y0

plot_12 = 47
plot_id = shapes[shapes['plot_12'] == plot_12].index.values[0]

#   function to plot a full map
def plot_map(sf, x_lim = None, y_lim = None, figsize = (11,9)):
    '''
    Plot map with lim coordinates
    '''
    plt.figure(figsize = figsize)
    id=0
    for shape in sf.shapeRecords():
        x = [i[0] for i in shape.shape.points[:]]
        y = [i[1] for i in shape.shape.points[:]]
        plt.plot(x, y, 'k')
        
        if (x_lim == None) & (y_lim == None):
            x0 = np.mean(x)
            y0 = np.mean(y)
            plt.text(x0, y0, id, fontsize=10)
        id = id+1
    
    if (x_lim != None) & (y_lim != None):     
        plt.xlim(x_lim)
        plt.ylim(y_lim)


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

#comuna_id = [0, 1, 2, 3, 4, 5, 6]
#plot_map_fill_multiples_ids("Multiple Shapes", 
                            #comuna_id, sf, color = 'r')
#plt.show()

#   function that plots based on name in place of id
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

south = [47, 73, 71]
plot_error_map(sf, 'Error Map: Date', [47, 73, 71], 'r')
plt.show()

#   need a process for the plot numbers to be taken immediately from error report dataframes

#   --PyPDF
#   https://www.justintodata.com/generate-reports-with-python/#pdf-directly
#   
#   
