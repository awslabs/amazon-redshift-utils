import os
import pandas as pd
import yaml

from functools import partial
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak,
    TableStyle,
    Table,
    Spacer,
    Image,
    SimpleDocTemplate,
    Paragraph,
    ListFlowable,
    ListItem,
)
from report_util import (
    styles,
    build_pdf_tables,
    df_to_np,
    first_page,
    later_pages,
    hist_gen,
    sub_yaml_vars,
)

g_stylesheet = styles()


def pdf_gen(report, summary=None):
    """This function formats the summary report using the content from report_content.yaml to populate the paragraphs,
       titles, and headers. The tables are populated via the Report param which has all the dataframes.

    @param report: Report object
    @param summary: list, replay summary

    """
    with open("report_content.yaml", "r") as stream:
        docs = yaml.safe_load(stream)

        style = g_stylesheet.get("styles")
        elems = []  # elements array used to build pdf structure
        pdf = SimpleDocTemplate(
            f"{report.replay_id}_report.pdf",
            pagesize=letter,
            leftMargin=0.75 * inch,
            rightMargin=0.75 * inch,
            topMargin=0.75 * inch,
            bottomMargin=0.75 * inch,
        )

        # title and subtitle and cluster info table
        elems.append(Paragraph(docs["title"], style["Title"]))
        elems.append(
            Paragraph(sub_yaml_vars(report, docs["subtitle"]), style["Heading4"])
        )
        cluster_info = pd.DataFrame.from_dict(report.cluster_details, orient="index")
        elems.append(
            Table(
                df_to_np(report.cluster_details.keys(), cluster_info.transpose()),
                hAlign="LEFT",
                style=g_stylesheet.get("table_style"),
            )
        )
        # replay summary
        if summary is not None:
            elems.append(Paragraph(f"Replay Summary", style["Heading4"]))
            elems.append(
                ListFlowable(
                    [ListItem(Paragraph(x, style["Normal"])) for x in summary],
                    bulletType="bullet",
                )
            )
            elems.append(Spacer(0, 5))

        elems.append(Paragraph(docs["report_paragraph"], style["Normal"]))

        # glossary section
        elems.append(Paragraph(docs["glossary_header"], style["Heading4"]))
        elems.append(Paragraph(docs["glossary_paragraph"], style["Normal"]))
        elems.append(
            ListFlowable(
                [ListItem(Paragraph(x, style["Normal"])) for x in docs["glossary"]],
                bulletType="bullet",
            )
        )
        elems.append(Spacer(0, 5))

        # access data section
        elems.append(Paragraph(docs["data_header"], style["Heading4"]))
        elems.append(
            Paragraph(sub_yaml_vars(report, docs["data_paragraph"]), style["Normal"])
        )
        elems.append(
            ListFlowable(
                [ListItem(Paragraph(x, style["Normal"])) for x in docs["raw_data"]],
                bulletType="bullet",
            )
        )
        elems.append(Spacer(0, 5))
        elems.append(
            Paragraph(
                sub_yaml_vars(report, docs["agg_data_paragraph"]), style["Normal"]
            )
        )

        # notes section
        elems.append(Paragraph(docs["notes_header"], style["Heading4"]))
        elems.append(Paragraph(docs["notes_paragraph"], style["Normal"]))
        elems.append(
            ListFlowable(
                [ListItem(Paragraph(x, style["Normal"])) for x in docs["notes"]],
                bulletType="bullet",
            )
        )

        elems.append(PageBreak())  # page 2: cluster details

        # query breakdown
        build_pdf_tables(elems, docs["query_breakdown"], report)
        elems.append(Spacer(0, 5))

        # histogram and description
        image_path = hist_gen(
            x_data=report.feature_graph["sec_start"],
            y_data=report.feature_graph["count"],
            title=docs["graph"].get("title"),
            x_label="Average Elapsed Time (s)",
        )

        desc = Paragraph(docs["graph"].get("paragraph"), style["Normal"])
        data = [[Image(image_path, width=300, height=200, hAlign="LEFT"), desc]]
        elems.append(
            Table(data, style=TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
        )
        elems.append(Spacer(0, 5))

        # cluster metrics table
        build_pdf_tables(elems, docs["cluster_metrics"], report)

        elems.append(PageBreak())  # page 3+ measure tables

        build_pdf_tables(
            elems, docs["measure_tables"], report
        )  # build 5 measure tables all at once

        # build pdf
        pdf.build(
            elems,
            onFirstPage=partial(first_page, report=report),
            onLaterPages=partial(later_pages, report=report),
        )
        os.remove(image_path)

        return pdf.filename
