# file: get_stat.py
# description: IZV 1. project
# data: 1.11 2020
# author: Alexandr Chalupnik
# email: <xchalu15@stud.fit.vutbr.cz>

import matplotlib.pyplot as plt
import numpy as np
import os 

def plot_stat(data_source, fig_location=None, show_figure=False):
    """
    Parameters
    ----------
    data_source : tuple(list(str), list(np.ndarray))
        prvni polozka: seznam retezcu, odpovídá názvům jednotlivých datových sloupců
        druha polozka: seznma NumPy poli, obsahuje data pro region
    fig_location : str
        slozka pro ulozeni grafu
        implicitne None, graf se neuklada
    show_figure : bool
        prinac pro zobraneni grafu
        implicitne False, graf se nezobrazi 
    """

    _, data = data_source
    
    regions = data[-1]
    _, i = np.unique(regions, return_index=True)
    regions = regions[np.sort(i)]

    ax_x = { r : [] for r in regions }  # regiony 
    ax_y = []
    dates = data[3].astype("datetime64[Y]")
    years = np.unique(dates)  # roky
    window_title = "Statistika dopravních nehod Policie ČR"

    for region in ax_x.keys():
        ax_y.append(np.nonzero(data[-1] == region)[0][0])
    ax_y.append(ax_y[-1]+len(data[-1]))

    for i, k in enumerate(ax_x.keys()):
        for year in years:
            ax_x[k].append((dates[ax_y[i]:ax_y[i+1]] == np.datetime64(year)).sum())

    year_data = np.array(list(ax_x.values())).T

    plt.style.use("seaborn")
    fig, ax = plt.subplots(nrows=len(years), ncols=1, figsize=(8.27, 11.69), constrained_layout=True, sharey=True)
    fig.suptitle(window_title, fontsize=14)
    fig.canvas.set_window_title(window_title)
    for subplot, year, data in zip(ax, years, year_data):
        set_subplot(subplot, year, ax_x.keys(), data)
    
    if show_figure:
        plt.show()
    if fig_location is not None:
        if not os.path.exists(os.path.dirname(fig_location)):
            os.makedirs(os.path.dirname(fig_location))
            
        plt.savefig(fig_location)


def set_subplot(subplt, year, x, y):
    """
    zajisti vykresleni podgrafu
    
    Parameters
    ----------
    subplt : pyplot.axes
        podgraf
    year : str
        rok
    x : list
        zkartky vypisovanych kraju
    y : list
        pocet dopravnich nehod pro kazdy kraj v roce year
    """
    
    subplt.set_title(f"Rok {year}")
    subplt.set_ylabel("Počet dopravních nehod")
    subplt.set_xlabel("Kraje")
    subplt.grid(which="major", axis="y", color="gray", linewidth=0.15)
    subplt.grid(b=False, which="major", axis="x")
    rects = subplt.bar(x,y)
    sorted_cols = sorted(y, reverse=True)
    for rect in rects:
        height = rect.get_height()
        subplt.annotate(f"{ sorted_cols.index(rect.get_height()) + 1 }", 
                        xytext=(0, 0), 
                        xy=(rect.get_x() + rect.get_width() / 2.0, height), 
                        textcoords="offset points", 
                        ha='center', va='bottom',
                        fontsize="large"
                        )
        
        subplt.annotate(f"{ height }", 
                        xytext=(0, -7), 
                        xy=(rect.get_x() + rect.get_width() / 2.0, height), 
                        textcoords="offset points", 
                        ha='center', va='center',
                        color="white",
                        fontsize="medium"
                        )