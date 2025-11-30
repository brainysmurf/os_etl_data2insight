import click
from .services import Service
from .sheets import GSheet
from .sheets import DuckDBParent
from .sheets import Directory
from .contexts import ServiceCtx, SourceCtx, TargetCtx
from .contexts import pass_service_ctx, pass_source_ctx, pass_target_ctx
import pathlib
from .utils import make_keyword_validator, TYPE_KEYWORDS, SOURCE_KEYWORDS
from .utils import find_value
import pandas as pd


@click.group()
@click.pass_context
def main(ctx):
    ctx.obj = ServiceCtx(Service())


@main.group()
@click.option("--type", "type_", type=click.Choice(TYPE_KEYWORDS.keys()))
@click.option(
    "--keyword",
    "keywords",
    multiple=True,
    type=(str, str),
    callback=make_keyword_validator(TYPE_KEYWORDS),
)
@click.option("--id", "id_", envvar="GSHEET_ID", show_envvar=True)
@pass_service_ctx
@click.pass_context
def target(ctx, service, type_, keywords, **kwargs):
    """
    Define the target source
    """
    if type_ == "gcloud":
        doc = GSheet(service.service, find_value("id_", keywords, kwargs))
    elif type_ == "screen":
        print("Can't handle screen yet!")
        doc = None
    elif type_ == "duckdb":
        doc = DuckDBParent()
    else:
        raise click.BadParameter(type_)

    ctx.obj = TargetCtx(doc)


@target.group()
@click.option("--type", "type_", type=click.Choice(TYPE_KEYWORDS.keys()))
@click.option(
    "--keyword",
    "keywords",
    multiple=True,
    type=(str, str),
    callback=make_keyword_validator(SOURCE_KEYWORDS),
)
@click.option("--id", "id_", envvar="GSHEET_ID", show_envvar=True)
@pass_service_ctx
@click.pass_context
def source(ctx, service, type_, keywords, **kwargs):
    """
    Define the data source
    """
    if type_ == "gcloud":
        doc = GSheet(service.service, find_value("id_", keywords, kwargs))
    else:
        raise click.BadParameter(type_)

    ctx.obj = SourceCtx(doc)


@source.command("scenario:housepoints:stage")
@click.option("--only")
@pass_target_ctx
@pass_source_ctx
def scenario_housepoints_setup(source, target, only):

    from .seeds import generate_housepoints

    for title, df in generate_housepoints():
        if (only and title == only) or only is None:
            target.document.write_tab(title, df)


@source.command("scenario:housepoints:transform")
@pass_target_ctx
@pass_source_ctx
def scenario_housepoints_calculate(source, target):

    # Loop through all sheets
    for sheet in source.document.spreadsheet.worksheets():
        # Get all values
        data = sheet.get_all_records()
        df = pd.DataFrame(data)

        # Table name: sheet title with spaces replaced
        table_name = sheet.title.replace(" ", "_")
        target.document.write_tab(table_name, df)

    result_df = target.document.document.database.execute(
        """
WITH house_points AS (
    SELECT
        h.id AS house_id,
        h.house_name,
        -- Convert place to points: 1 → 6, 2 → 5, ..., 6 → 1
        CASE p.place
            WHEN 1 THEN 6
            WHEN 2 THEN 5
            WHEN 3 THEN 4
            WHEN 4 THEN 3
            WHEN 5 THEN 2
            WHEN 6 THEN 1
            ELSE 0
        END AS points
    FROM placements AS p
    JOIN houses AS h
        ON p.house_id = h.id
    JOIN races AS r
        ON p.race_id = r.id
)
SELECT
    house_id,
    house_name,
    SUM(points) AS total_points,
    COUNT(*) AS races_participated
FROM house_points
GROUP BY house_id, house_name
ORDER BY total_points DESC;
        """
    ).fetchdf()

    denormalized_df = target.document.document.database.execute(
        """
SELECT
    r.id AS race_id,
    r.name,
    h.id AS house_id,
    h.house_name,
    p.place,
    -- Convert place to points
    CASE p.place
        WHEN 1 THEN 6
        WHEN 2 THEN 5
        WHEN 3 THEN 4
        WHEN 4 THEN 3
        WHEN 5 THEN 2
        WHEN 6 THEN 1
        ELSE 0
    END AS points
FROM placements AS p
JOIN races AS r
    ON p.race_id = r.id
JOIN houses AS h
    ON p.house_id = h.id
ORDER BY r.id, p.place;
        """
    ).fetchdf()

    source.document.write_tab("results", result_df)

    source.document.write_tab("placements_denormalized", denormalized_df)


# @main.command()
# @click.option("--id", "id_", envvar="GSHEET_ID", show_envvar=True)
# @click.option(
#     "--json_path",
#     "json_path",
#     type=click.Path(),
#     envvar="PATH_TO_JSON",
#     show_envvar=True,
# )
# @pass_service_ctx
# @click.pass_context
# def source(ctx, service_ctx: ServiceCtx, id_: str, json_path: str):

#     if id_ is not None:
#         source = GSheet(service_ctx.service, id_, open_=False)

#     elif json_path is not None:
#         source = ...

#     ctx.obj = SourceCtx(source)


# @main.group()
# @click.option("--path", type=click.Path(), required=True)
# @pass_service_ctx
# @click.pass_context
# def local_files(ctx, service_ctx: ServiceCtx, path: pathlib.Path):
#     path = pathlib.Path(path)
#     source = Directory(path)
#     document = source.open()
#     ctx.obj = SourceCtx(document)


# @local_files.command()
# @pass_source_ctx
# def housepoint_seeds(source_ctx):
#     doc = source_ctx.document

#     records = []
#     for id_ in range(1000, 1000 * 10):
#         record = {"id": id_, "name": "Name", "house": "House"}
#         records.append(record)

#     doc.save_records("students", records)


# @local_files.command()
# @pass_source_ctx
# def housepoints(source_ctx):
#     doc = source_ctx.document
#     print(doc)
