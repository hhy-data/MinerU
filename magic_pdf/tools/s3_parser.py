import os

from loguru import logger

import magic_pdf.model as model_config
from magic_pdf.data.data_reader_writer import S3DataWriter
from magic_pdf.config.enums import SupportedPdfParseMethod
from magic_pdf.config.make_content_config import DropMode, MakeMode
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.libs.draw_bbox import draw_char_bbox
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.operators.models import InferenceResult
from magic_pdf.tools.common import convert_pdf_bytes_to_bytes_by_pymupdf




def prepare_env_for_s3(output_prefix, pdf_file_name, method):
    local_parent_dir = os.path.join(output_prefix, pdf_file_name, method)

    image_prefix = os.path.join(str(local_parent_dir), 'images')
    md_prefix = local_parent_dir
    return image_prefix, md_prefix


def do_parse_with_s3(
    output_prefix,
    pdf_file_name,
    pdf_bytes,
    model_list,
    parse_method,
    debug_able,
    ak,
    sk,
    endpoint,
    bucket,
    f_dump_md=True,
    f_dump_middle_json=True,
    f_dump_model_json=True,
    f_dump_orig_pdf=True,
    f_dump_content_list=True,
    f_make_md_mode=MakeMode.MM_MD,
    start_page_id=0,
    end_page_id=None,
    lang=None,
    layout_model=None,
    formula_enable=None,
    table_enable=None,
):
    if debug_able:
        logger.warning('debug mode is on')

    pdf_bytes = convert_pdf_bytes_to_bytes_by_pymupdf(
        pdf_bytes, start_page_id, end_page_id
    )

    image_prefix, md_prefix = prepare_env_for_s3(output_prefix, pdf_file_name, parse_method)

    image_writer = S3DataWriter(image_prefix, bucket, ak, sk, endpoint)
    md_writer = S3DataWriter(md_prefix, bucket, ak, sk, endpoint)

    ds = PymuDocDataset(pdf_bytes, lang=lang)

    if len(model_list) == 0:
        if model_config.__use_inside_model__:
            if parse_method == 'auto':
                if ds.classify() == SupportedPdfParseMethod.TXT:
                    infer_result = ds.apply(
                        doc_analyze,
                        ocr=False,
                        lang=ds._lang,
                        layout_model=layout_model,
                        formula_enable=formula_enable,
                        table_enable=table_enable,
                    )
                    pipe_result = infer_result.pipe_txt_mode(
                        image_writer, debug_mode=True, lang=ds._lang
                    )
                else:
                    infer_result = ds.apply(
                        doc_analyze,
                        ocr=True,
                        lang=ds._lang,
                        layout_model=layout_model,
                        formula_enable=formula_enable,
                        table_enable=table_enable,
                    )
                    pipe_result = infer_result.pipe_ocr_mode(
                        image_writer, debug_mode=True, lang=ds._lang
                    )

            elif parse_method == 'txt':
                infer_result = ds.apply(
                    doc_analyze,
                    ocr=False,
                    lang=ds._lang,
                    layout_model=layout_model,
                    formula_enable=formula_enable,
                    table_enable=table_enable,
                )
                pipe_result = infer_result.pipe_txt_mode(
                    image_writer, debug_mode=True, lang=ds._lang
                )
            elif parse_method == 'ocr':
                infer_result = ds.apply(
                    doc_analyze,
                    ocr=True,
                    lang=ds._lang,
                    layout_model=layout_model,
                    formula_enable=formula_enable,
                    table_enable=table_enable,
                )
                pipe_result = infer_result.pipe_ocr_mode(
                    image_writer, debug_mode=True, lang=ds._lang
                )
            else:
                logger.error('unknown parse method')
                exit(1)
        else:
            logger.error('need model list input')
            exit(2)
    else:

        infer_result = InferenceResult(model_list, ds)
        if parse_method == 'ocr':
            pipe_result = infer_result.pipe_ocr_mode(
                image_writer, debug_mode=True, lang=ds._lang
            )
        elif parse_method == 'txt':
            pipe_result = infer_result.pipe_txt_mode(
                image_writer, debug_mode=True, lang=ds._lang
            )
        else:
            if ds.classify() == SupportedPdfParseMethod.TXT:
                pipe_result = infer_result.pipe_txt_mode(
                        image_writer, debug_mode=True, lang=ds._lang
                    )
            else:
                pipe_result = infer_result.pipe_ocr_mode(
                        image_writer, debug_mode=True, lang=ds._lang
                    )


    if f_dump_md:
        pipe_result.dump_md(
            md_writer,
            f'{pdf_file_name}.md',
            image_prefix,
            drop_mode=DropMode.NONE,
            md_make_mode=f_make_md_mode,
        )

    if f_dump_middle_json:
        pipe_result.dump_middle_json(md_writer, f'{pdf_file_name}_middle.json')

    if f_dump_model_json:
        infer_result.dump_model(md_writer, f'{pdf_file_name}_model.json')

    if f_dump_orig_pdf:
        md_writer.write(
            f'{pdf_file_name}_origin.pdf',
            pdf_bytes,
        )

    if f_dump_content_list:
        pipe_result.dump_content_list(
            md_writer,
            f'{pdf_file_name}_content_list.json',
            image_prefix
        )

    logger.info(f'output prefix is {md_prefix}')
