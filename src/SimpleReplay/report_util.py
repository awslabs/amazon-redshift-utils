import matplotlib.colors as mcolors
import numpy as np
import re

from datetime import datetime
from matplotlib import pyplot as plt
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors as rcolors
from reportlab.platypus import PageBreak, TableStyle, Table, Paragraph
from reportlab.rl_settings import defaultPageSize


class Report:
    """
        A class used to represent a Report

        Attributes
        ----------
        cluster : dict
            cluster dictionary
        replay_id : str
            replay identifier
        bucket : dict
            bucket dictionary
        path : str
            replay path for s3
        complete : bool
            whether the replay was completed
        cluster_details : dict
            dictionary of cluster details
        query_columns : list
            list of column names for query breakdown table
        metrics_columns : list
            list of column names for cluster metrics table
        measure_columns : list
            list of columns names for measure tables
        tables : dict
            dictionary of table
        feature_graph : dataframe
            dataframe for feature graph (hist)
        """

    def __init__(self, cluster_dict, replay_id, s3_dict, path, tag='', complete=True):
        self.cluster = cluster_dict
        self.replay_id = replay_id
        self.bucket = s3_dict
        self.path = path
        self.complete = complete

        # cluster info section
        self.cluster_details = {"Cluster ID": self.cluster.get('id'),
                                "Start Time": self.cluster.get('start_time'),
                                "End Time": self.cluster.get('end_time'),
                                "Instance Type": self.cluster.get('instance'),
                                "Nodes": self.cluster.get('num_nodes'),
                                "Replay Tag": tag,
                                }

        self.query_columns = ['Statement Type', 'Total Count', 'Aborted Count']
        self.metrics_columns = ['Measure', 'Avg(s)', 'Std Dev(s)', 'P25(s)', 'P50(s)', 'P75(s)', 'P99(s)']
        self.measure_columns = ['User', 'Query Count', 'Avg(s)', 'Std Dev(s)', 'P25(s)', 'P50(s)',
                                'P75(s)', 'P99(s)']

        # maps table titles to tables, matching column names, tables types, and query result file
        self.tables = {
            'Query Breakdown': {'data': None,
                                'columns': self.query_columns,
                                'type': 'breakdown',
                                'sql': 'statement_types'},
            'Cluster Metrics': {'data': None,
                                'columns': self.metrics_columns,
                                'type': 'metric',
                                'sql': 'cluster_level_metrics'},
            'Query Latency': {'data': None,
                              'columns': self.measure_columns,
                              'type': 'measure',
                              'sql': 'query_distribution',
                              'graph': None},
            'Compile Time': {'data': None,
                             'columns': self.measure_columns,
                             'type': 'measure',
                             'sql': 'query_distribution',
                             'graph': None},
            'Queue Time': {'data': None,
                           'columns': self.measure_columns,
                           'type': 'measure',
                           'sql': 'query_distribution',
                           'graph': None},
            'Execution Time': {'data': None,
                               'columns': self.measure_columns,
                               'type': 'measure',
                               'sql': 'query_distribution',
                               'graph': None},
            'Commit Queue Time': {'data': None,
                                  'columns': self.measure_columns,
                                  'type': 'measure',
                                  'sql': 'query_distribution',
                                  'graph': None},
            'Commit Time': {'data': None,
                            'columns': self.measure_columns,
                            'type': 'measure',
                            'sql': 'query_distribution',
                            'graph': None},
            'Aborted Queries': {'data': None,
                                'columns': self.measure_columns,
                                'type': 'measure',
                                'sql': ''}
        }
        # this attribute is for the page 2 graph
        self.feature_graph = {'Query Latency': None}


def first_page(canvas, doc, report):
    """Specifies footer for first page"""

    canvas.saveState()
    canvas.setFont('Helvetica', 9)
    canvas.drawCentredString(4.25 * inch, 0.5 * inch, "\u00A9 2021, Amazon Web Services, Inc. or its Affiliates. All "
                                                      "rights reserved.")
    canvas.drawCentredString(4.25 * inch, 0.35 * inch, "Amazon Confidential and Trademark.")
    if not report.complete:
        canvas.setFillColorRGB(1, 0, 0)
        canvas.drawCentredString(4.25 * inch, 10.55 * inch, "Simple Replay was terminated. The results displayed in "
                                                            "this report are incomplete and may not be comparable to "
                                                            "other replay data.")
        canvas.setFillColorRGB(0, 0, 0)

    canvas.restoreState()


def later_pages(canvas, doc, report):
    """Specifies header/footer for additional pages"""

    canvas.saveState()
    canvas.setFont('Helvetica', 9)
    if report.cluster_details['Replay Tag'] == '':
        canvas.drawCentredString(4.25 * inch, 10.5 * inch, f"Cluster id: {report.cluster.get('id')}      "
                                                           f"Report generated: {datetime.today().date()}")
    else:
        canvas.drawCentredString(4.25 * inch, 10.5 * inch, f"Cluster id: {report.cluster.get('id')}      "
                                                           f"Replay tag: {report.cluster_details['Replay Tag']}      "
                                                           f"Report generated: {datetime.today().date()}")

    canvas.drawString(0.5 * inch, 0.5 * inch, f"{doc.page}")
    canvas.drawCentredString(4.25 * inch, 0.5 * inch, "\u00A9 2021, Amazon Web Services, Inc. or its Affiliates. All "
                                                      "rights reserved.")
    canvas.drawCentredString(4.25 * inch, 0.35 * inch, "Amazon Confidential and Trademark.")
    canvas.drawImage('resources/logo.png', 7 * inch, 0.25 * inch, width=80, height=45, mask=None)
    canvas.restoreState()


def hist_gen(x_data, y_data, title, x_label):
    """Generates a histogram for give table data

    @param x_data: pandas series, x axis data
    @param y_data: pandas series, y axis data
    @param title: str, title of graph
    @param x_label: str, x label for graph
    @return: str, file name

     """
    file = f"{title.replace(' ', '')}.png"  # set filename for saving
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(x_data, y_data, align='center', width=(max(x_data) - min(x_data)) / float(len(x_data)) * 0.8,
           color=mcolors.CSS4_COLORS['darkorange'])
    ax.set_xlabel(x_label)
    ax.set_ylabel("Count (log)")
    ax.set_yscale('log')
    ax.set_title(title)
    plt.savefig(file)
    return file


def df_to_np(heading, df):
    """Maps a data frame to a numpy array

    :param heading: list, column names
    :param df: Dataframe, data being converted
     """
    output = []
    if len(heading) > 0:
        output.append(heading)
    frame = df.reset_index(drop=True)
    frame = frame.truncate(after=100)
    for e in np.array(frame).tolist():
        output.append(e)
    return output


def styles():
    """ Specifies style guidelines for pdf report and column formatting

    @return: dict, style guidelines
    """
    page_height = defaultPageSize[1]
    page_width = defaultPageSize[0]
    squidink = rcolors.Color(red=(35.0 / 255), green=(47.0 / 255), blue=(62.0 / 255))

    columns = {'Measure': 'measure_type',
               'User': 'usename',
               'Service Class': 'service_class',
               'Queue': 'queue',
               'Aborted Count': 'aborted',
               'Query Count': 'query_count',
               'P25(s)': 'p25_s',
               'P50(s)': 'p50_s',
               'P75(s)': 'p75_s',
               'P90(s)': 'p90_s',
               'P95(s)': 'p95_s',
               'P99(s)': 'p99_s',
               'Max(s)': 'max_s',
               'Avg(s)': 'avg_s',
               'Std Dev(s)': 'std_s',
               'Statement Type': 'statement_type',
               'Burst Count': 'count_cs',
               'Total Count': 'total_count'
               }

    # styling definitions for pdf
    table_style = TableStyle(
        [('LINEABOVE', (0, 1), (-1, -1), 0.25, rcolors.black),
         ('LINEBELOW', (0, -1), (-1, -1), .25, rcolors.black),
         ('GRID', (0, 0), (-1, -1), 0.5, rcolors.black),
         ('BACKGROUND', (0, 0), (-1, 0), squidink),
         ('TEXTCOLOR', (0, 0), (-1, 0), rcolors.white),
         ('FONTSIZE', (0, 1), (-1, -1), 9)
         ]
    )
    style = getSampleStyleSheet()
    style['Heading4'].fontName = 'Helvetica-Bold'
    style['Heading4'].spaceAfter = 1
    style['Normal'].spaceAfter = 1.5
    style['Normal'].textSize = 8
    style['Normal'].borderPadding = 4
    style.add(ParagraphStyle(name='incomplete',
                             fontFamily='Helvetica',
                             fontSize=10,
                             textColor='red'))

    return {'page_height': page_height,
            'page_width': page_width,
            'columns': columns,
            'table_style': table_style,
            'styles': style}


def sub_yaml_vars(report, paragraph, replace_dict=None):
    """

    @param report: Report object
    @param paragraph: str, report paragraph that is being edited
    @param replace_dict: dict, specific elements to replace
    @return:
    """
    if replace_dict is None:
        replace_dict = {}
    replace_map = {
        '{CLUSTER_ID}': report.cluster.get('id'),
        '{S3_BUCKET}': report.bucket.get('bucket_name'),
        '{REPLAY_ID}': report.replay_id
    }
    if len(replace_dict) == 0:
        replacements = re.findall("{([^}]+)}", paragraph)
        for r in replacements:
            r = "{" + r + "}"
            paragraph = re.sub(r, replace_map.get(r), paragraph)
    else:
        for a in replace_dict:
            paragraph = re.sub(a, str(replace_dict.get(a)), paragraph)

    return paragraph


def build_pdf_tables(story, tables, report):
    """ Builds formatted tables sections for a list of tables

    @param story: list, pdf elements
    @param tables: list, tables to build
    @param report: Report object
    """
    stylesheet = styles()
    style = stylesheet.get('styles')
    table_style = stylesheet.get('table_style')
    for t, d in tables.items():
        table_name = d.get('title')
        cols = report.tables.get(table_name).get('columns')
        data = report.tables.get(table_name).get('data')

        story.append(Paragraph(table_name, style['Heading4']))
        story.append(Paragraph(d.get('paragraph'), style['Normal']))
        if 'note' in d:
            story.append(Paragraph(f"<sub>{d.get('note')}</sub>", style['Normal']))
        story.append(Table(df_to_np(cols, data), hAlign='LEFT', style=table_style))
        if len(df_to_np(cols, data)) > 15:
            story.append(PageBreak())

        # to add graphs for each table: call hist_gen on associated graph data
