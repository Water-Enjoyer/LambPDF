import time
import boto3
import pypdf
import pdfrw
import logging
import tempfile
import functools
from io import BytesIO
from fillpdf import fillpdfs
from collections import defaultdict
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from reportlab.pdfgen import canvas
logger = logging.getLogger(__name__)


class PDFProcessingError(Exception):
    pass


class S3Error(Exception):
    pass


class TextractError(Exception):
    pass


def with_temp_files(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            with tempfile.NamedTemporaryFile(delete=True) as temp_in, \
                    tempfile.NamedTemporaryFile(delete=True) as temp_out:
                return func(*args, temp_in=temp_in, temp_out=temp_out, **kwargs)
        except Exception as e:
            logger.error(f"Temp file processing failed: {e}")
            raise PDFProcessingError("PDF processing failed") from e
    return wrapper


class LambPDF():
    def __init__(
            self,
            s3_client: BaseClient | None = None,
            region: str | None = None,

    ):
        self.s3_client: BaseClient = s3_client or boto3.client('s3')
        if not region:
            region = "us-east-2"
        self.region = region

    def download_pdf_from_s3(
        self,
        bucket_name: str,
        s3_key: str,
    ) -> BytesIO:
        """
        Downloads PDF from S3 and returns BytesIO
        """
        try:
            response = self.s3_client.get_object(
                Bucket=bucket_name,
                Key=s3_key
            )
            return BytesIO(
                response['Body'].read()
            )
        except ClientError as e:
            logger.error(f"Failed to download PDF from S3: {e}")
            raise S3Error(
                f"Failed to retrieve {s3_key} from {bucket_name}") from e

    def upload_pdf_to_s3(
            self,
            bucket_name: str,
            s3_key: str,
            pdf_buffer: BytesIO
    ) -> dict:
        """
        Uploads PDF buffer to S3
        """
        try:
            return self.s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=pdf_buffer.getvalue()
            )
        except ClientError as e:
            logger.error(f"Failed to upload PDF to S3: {e}")
            raise S3Error(f"Failed to upload {s3_key} to {bucket_name}") from e

    @with_temp_files
    def fill_pdf_form(
        self,
        template_buffer: BytesIO,
        data: dict,
        *,
        temp_in: tempfile.NamedTemporaryFile,
        temp_out: tempfile.NamedTemporaryFile,
        annot_flatten: bool = False,
    ) -> BytesIO:
        """
        Merges data to template_buffer form fields via fillpdfs.write_fillable_pdf
        Returns BytesIO of the filled PDF
        """
        temp_in.write(
            template_buffer.read()
        )
        temp_in.flush()
        fillpdfs.write_fillable_pdf(
            temp_in.name,
            temp_out.name,
            data,
            flatten=annot_flatten
        )
        temp_out.seek(0)
        output_buffer = BytesIO(temp_out.read())
        return output_buffer

    @with_temp_files
    def flatten_as_images(
        self,
        input_buffer: BytesIO,
        *,
        temp_in: tempfile.NamedTemporaryFile,
        temp_out: tempfile.NamedTemporaryFile,
        retain_x_scale: bool = True,
    ) -> BytesIO:
        """
        Flattens the input_buffer PDF as images
        Returns BytesIO of the flattened PDF
        """
        temp_in.write(
            input_buffer.read()
        )
        temp_in.flush()

        fillpdfs.flatten_pdf(
            temp_in.name, temp_out.name, as_images=True)

        output_buffer = BytesIO()
        if retain_x_scale:
            original_pdf = pypdf.PdfReader(temp_in.name)
            original_page = original_pdf.pages[0]
            original_width = float(original_page.mediabox.width)
            flattened_pdf = pypdf.PdfReader(temp_out.name)
            output_pdf = pypdf.PdfWriter()
            for page in flattened_pdf.pages:
                current_width = float(page.mediabox.width)
                scale_x = original_width / current_width
                page.scale_by(scale_x)
                output_pdf.add_page(page)
            output_pdf.write(output_buffer)
            output_buffer.seek(0)
            return output_buffer
        temp_out.seek(0)
        output_buffer.write(temp_out.read())
        return output_buffer

    def append_blank_page(
        self,
        input_buffer: BytesIO,
    ) -> BytesIO:
        """
        Adds a blank page to the end of input_buffer PDF
        Returns BytesIO of the PDF with a blank page
        """
        try:
            reader = pdfrw.PdfReader(fdata=input_buffer.getvalue())
            if not reader.pages:
                raise PDFProcessingError("Input PDF contains no pages")

            first_page = reader.pages[0]
            page_size = (
                float(first_page.MediaBox[2]),  # width
                float(first_page.MediaBox[3]),  # height
            )
            blank_page = pdfrw.PdfDict(
                Type=pdfrw.PdfObject('/Page'),
                MediaBox=[
                    0,
                    0,
                    page_size[0],
                    page_size[1]
                ],
            )

            writer = pdfrw.PdfWriter()
            writer.addpages(reader.pages)
            writer.addpage(blank_page)

            output_buffer = BytesIO()
            writer.write(output_buffer)
            output_buffer.seek(0)
            return output_buffer

        except Exception as e:
            logger.error(f"Failed to add blank page: {e}")
            raise PDFProcessingError("Failed to add blank page") from e

    def concatenate_pdfs(
        self,
        pdf_buffers: list[BytesIO],
    ) -> BytesIO:
        """
        Merges list of PDFs into a single file
        Returns BytesIO of the merged PDF
        """
        if not pdf_buffers:
            raise ValueError("At least one PDF buffer must be provided")

        try:
            writer = pdfrw.PdfWriter()
            for buffer in pdf_buffers:
                if buffer.getbuffer().nbytes == 0:
                    logger.warning("Skipping empty PDF buffer in merge")
                    continue

                reader = pdfrw.PdfReader(fdata=buffer.getvalue())
                writer.addpages(reader.pages)

            output_buffer = BytesIO()
            writer.write(output_buffer)
            output_buffer.seek(0)
            return output_buffer
        except Exception as e:
            logger.error(f"Failed to concatenate PDFs: {e}")
            raise PDFProcessingError("Failed to concatenate PDFs") from e

    def duplicate_pages(
            self,
            pdf_buffer: BytesIO,
            copies: int,
    ) -> BytesIO:
        """
        Duplicates pages in order of the input PDF
        Returns BytesIO of the PDF with duplicated pages
        """
        if copies < 1:
            raise ValueError("Copies must be a positive integer")

        try:
            reader = pdfrw.PdfReader(fdata=pdf_buffer.getvalue())
            writer = pdfrw.PdfWriter()

            for _ in range(copies):
                writer.addpages(reader.pages)

            output_buffer = BytesIO()
            writer.write(output_buffer)
            output_buffer.seek(0)
            return output_buffer
        except Exception as e:
            logger.error(f"Failed to duplicate pages: {e}")
            raise PDFProcessingError("Failed to duplicate pages") from e

    def embed_form_annotations(
        self,
        background_buffer: BytesIO,
        form_buffer: BytesIO,
    ) -> BytesIO:
        try:
            background_pdf = pdfrw.PdfReader(
                fdata=background_buffer.getvalue())
            form_pdf = pdfrw.PdfReader(fdata=form_buffer.getvalue())

            for bg_page, form_page in zip(background_pdf.pages, form_pdf.pages):
                if form_page.Annots:
                    bg_page.Annots = (bg_page.Annots or []) + form_page.Annots

            if hasattr(form_pdf.Root, 'AcroForm') and form_pdf.Root.AcroForm:
                if not hasattr(background_pdf.Root, 'AcroForm') or not background_pdf.Root.AcroForm:
                    background_pdf.Root.AcroForm = pdfrw.PdfDict()
                for key, value in form_pdf.Root.AcroForm.items():
                    if key == 'Fields':
                        if 'Fields' not in background_pdf.Root.AcroForm:
                            background_pdf.Root.AcroForm['Fields'] = pdfrw.PdfArray(
                            )
                        background_pdf.Root.AcroForm['Fields'].extend(value)
                    else:
                        background_pdf.Root.AcroForm[key] = value
                background_pdf.Root.AcroForm[pdfrw.PdfName(
                    'NeedAppearances')] = pdfrw.PdfObject('true')

            output_buffer = BytesIO()
            pdfrw.PdfWriter().write(output_buffer, background_pdf)
            output_buffer.seek(0)
            return output_buffer

        except Exception as e:
            logger.error(f"Failed to embed form annotations: {e}")
            raise PDFProcessingError("Failed to embed form annotations") from e

    def textract_pdf_from_s3(
        self,
        bucket_name: str,
        s3_key: str,
        print_progress: bool = False,
    ) -> dict[int, list[dict]]:
        client = boto3.client(
            'textract',
            region_name=self.region,
        )
        try:
            response = client.start_document_text_detection(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': bucket_name,
                        'Name': s3_key
                    }
                }
            )
        except ClientError as e:
            logger.error(f"Failed to start Textract job: {e}")
            raise TextractError(
                f"Failed to start Textract job for {s3_key} in {bucket_name}") from e

        job_id = response['JobId']
        final_output_by_page = defaultdict(list)
        next_token = None

        if print_progress:
            print(f"\nJob Started, ID: {job_id}")
            print("=" * 80)
        while True:
            try:
                status = client.get_document_text_detection(JobId=job_id)
            except ClientError as e:
                logger.error(f"Failed to get Textract job status: {e}")
                raise TextractError(
                    f"Failed to get Textract job status for {job_id}") from e

            job_status = status['JobStatus']
            if job_status in ['SUCCEEDED', 'FAILED']:
                break

            if print_progress:
                print(
                    f"Waiting for job to complete... Current status: {job_status}"
                )
            time.sleep(5)

        if job_status == 'FAILED':
            logger.error(f"Textract job failed for {job_id}")
            raise TextractError(f"Textract job failed for {job_id}")

        while True:
            response_kwargs = {'JobId': job_id}
            if next_token:
                response_kwargs['NextToken'] = next_token

            try:
                output = client.get_document_text_detection(**response_kwargs)
            except ClientError as e:
                logger.error(f"Failed to get Textract job output: {e}")
                raise TextractError(
                    f"Failed to get Textract job output for {job_id}") from e

            for block in output['Blocks']:
                page_num = block.get('Page', 0)
                final_output_by_page[page_num].append(block)

            next_token = output.get('NextToken', None)
            if not next_token:
                break

        if print_progress:
            print("=" * 80)
            print("Textract Successful\n")

        return dict(final_output_by_page)

    def overlay_ocr_on_pdf(
        self,
        input_buffer: BytesIO,
        blocks_by_page: dict[int, list[dict]]
    ) -> None:
        output_buffer = BytesIO()
        c = canvas.Canvas(output_buffer)

        pdf_reader = pypdf.PdfReader(input_buffer)
        pdf_writer = pypdf.PdfWriter()

        for i, page in enumerate(pdf_reader.pages):
            print(f"Generating Page -> {i + 1}")

            page_height = float(page.mediabox[-1])
            page_width = float(page.mediabox[-2])

            c.setPageSize((page_width, page_height))
            c.setFillColorRGB(1, 1, 1, 0)

            page_blocks = blocks_by_page.get(i + 1, [])

            for block in page_blocks:
                if block['BlockType'] in ['WORD', 'LINE']:
                    x = block['Geometry']['BoundingBox']['Left'] * page_width
                    y = page_height - \
                        (block['Geometry']['BoundingBox']['Top'] * page_height)
                    text = block['Text']

                    x -= 2  # fine-tuning
                    y -= 9  # fine-tuning

                    c.drawString(x, y, text)

            c.showPage()
        c.save()

        overlay_reader = pypdf.PdfReader(output_buffer)
        print("Merging original .pdf and overlay .pdf to final")
        for i, page in enumerate(pdf_reader.pages):
            print(f"Merging Page -> {i + 1}")
            overlay_page = overlay_reader.pages[i]
            page.merge_page(overlay_page)
            pdf_writer.add_page(page)

        final_output_buffer = BytesIO()
        pdf_writer.write(final_output_buffer)
        final_output_buffer.seek(0)
        return final_output_buffer

# old, deprecated

    def get_pdf_buffer_from_s3(
            self,
            bucket_name: str,
            s3_key: str
    ) -> BytesIO:
        """
        Returns BytesIO of the PDF from S3
        """
        return self.download_pdf_from_s3(
            bucket_name,
            s3_key
        )

    def write_pdf_buffer_to_s3(
            self,
            bucket_name: str,
            s3_key: str,
            pdf_buffer: BytesIO
    ) -> dict:
        """
        Writes PDF buffer to S3
        """
        return self.upload_pdf_to_s3(
            bucket_name,
            s3_key,
            pdf_buffer
        )

    def write_fillable_pdf_buffer(
        self,
        template_buffer: BytesIO,
        data: dict,
        annot_flatten: bool = False
    ) -> BytesIO:
        """
        Merges data to template_buffer form fields via fillpdfs.write_fillable_pdf
        Returns BytesIO of the filled PDF
        """
        return self.fill_pdf_form(
            template_buffer,
            data,
            annot_flatten=annot_flatten
        )

    def flatten_pdf_buffer_as_images(
        self,
        input_buffer: BytesIO,
        retain_x_scale: bool = True,
    ) -> BytesIO:
        """
        Flattens the input_buffer PDF as images
        Returns BytesIO of the flattened PDF
        """
        return self.flatten_as_images(
            input_buffer,
            retain_x_scale=retain_x_scale
        )

    def add_blank_page(
            self,
            input_buffer: BytesIO,
    ) -> BytesIO:
        """
        Adds a blank page to the end of input_buffer PDF
        Returns BytesIO of the PDF with a blank page
        """
        return self.append_blank_page(
            input_buffer
        )

    def merge_pdfs(
        self,
        pdf_buffers: list[BytesIO],
    ) -> BytesIO:
        """
        Merges list of PDFs into a single file
        Returns BytesIO of the merged PDF
        """
        return self.concatenate_pdfs(pdf_buffers)

    def make_copies(
            self,
            pdf_buffer: BytesIO,
            copies: int,
    ) -> BytesIO:
        """
        Makes copies in order of the input PDF
        Returns BytesIO of the PDF with copies
        """
        return self.duplicate_pages(pdf_buffer, copies)

    def overlay_form_fields(
        self,
        background_buffer: BytesIO,
        form_buffer: BytesIO,
    ) -> BytesIO:
        """
        Overlays form_buffer on top of background_buffer
        Returns BytesIO of the PDF with form fields overlaid
        """
        return self.embed_form_annotations(
            background_buffer,
            form_buffer
        )
