"""
Title: flagging_report.py

Goal: The workflow code used to create the flagging reports every two weeks.
"""

import pandas as pd
import shapefile as shf
import matplotlib.pyplot as plt
import numpy as np

from fpdf import FPDF

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
air_error = air_error.merge(plot_design, how='left', on='plot')
soil_error = soil_error.merge(plot_design, how='left', on='plot')

#   group the merged table by the plot numbers and the error types
group_air = air_error.groupby(by=['plot', 'error_type']).size()
group_soil = soil_error.groupby(by=['plot', 'error_type']).size()

#   put the air and soil dataframes back together
air_plt_list = group_air.index.get_level_values('plot').tolist()
soil_plt_list = group_soil.index.get_level_values('plot').tolist()

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
        if(i <= 76):
            plot_id.append(df[df['plot_12'] == i]
                            .index.values[0])
    plot_map_fill_multiples_ids(title, plot_id, sf, 
                                       x_lim = None, 
                                       y_lim = None, 
                                       figsize = (11,9), 
                                       color = color)

#   create error map graphic

plot_error_map(sf, 'Air Error Map: 10/01/21 - 12/13/21', air_plt_list, 'r')
plt.savefig(r"D:\sample_biodiversitree\data\error_reports\error_report_figs\air_fig_1221.png")

plot_error_map(sf, 'Soil Error Map: 10/01/21 - 12/13/21', soil_plt_list, 'r')
plt.savefig(r"D:\sample_biodiversitree\data\error_reports\error_report_figs\soil_fig_1221.png")

#   create error table -> logger|plot|error_type|error_count
air_plt_df = group_air.reset_index()
soil_plt_df = group_soil.reset_index()


#   function that puts dataframes into pdf chart format
def output_df_to_pdf(pdf, df):
    # A cell is a rectangular area, possibly framed, which contains some text
    # Set the width and height of cell
    table_cell_width = 25
    table_cell_height = 6
    # Select a font as Arial, bold, 8
    pdf.set_font('Arial', 'B', 8)
    
    # Loop over to print column names
    cols = df.columns
    for col in cols:
        pdf.cell(table_cell_width, table_cell_height, col, align='C', border=1)
    # Line break
    pdf.ln(table_cell_height)
    # Select a font as Arial, regular, 10
    pdf.set_font('Arial', '', 10)
    # Loop over to print each data in the table
    for row in df.itertuples():
        for col in cols:
            value = str(getattr(row, col))
            pdf.cell(table_cell_width, table_cell_height, value, align='C', border=1)
        pdf.ln(table_cell_height)

#   create a pdf document for the error report
# 1. Set up the PDF doc basics
pdf = FPDF()
pdf.add_page()
pdf.set_font('Arial', 'B', 16)

# 2. Layout the PDF doc contents
## Title
pdf.cell(40, 10, 'Error Report: 10/01/21 - 12/13/21')
## Line breaks
pdf.ln(20)
### Use the function defined earlier to print the DataFrame as a table on the PDF 
output_df_to_pdf(pdf, air_plt_df)
## Line breaks
pdf.ln(20)
## Show table of historical summary data
output_df_to_pdf(pdf, soil_plt_df)
## Line breaks
pdf.ln(20)
## Image
pdf.image(r"D:\sample_biodiversitree\data\error_reports\error_report_figs\air_fig_1221.png")
pdf.ln(20)
pdf.image(r"D:\sample_biodiversitree\data\error_reports\error_report_figs\soil_fig_1221.png")
# 3. Output the PDF file
pdf.output('error_report_1221.pdf', 'F')
