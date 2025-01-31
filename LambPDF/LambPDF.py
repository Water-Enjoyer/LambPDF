import boto3
from botocore.client import BaseClient
import pypdf
import pdfrw
import tempfile
from io import BytesIO
from fillpdf import fillpdfs


class LambPDF():
    def __init__(self, s3_client: BaseClient | None = None):
        self.s3_client: BaseClient = s3_client or boto3.client('s3')

    def get_pdf_buffer_from_s3(
            self,
            bucket_name: str,
            s3_key: str
    ) -> BytesIO:
        """
        Returns BytesIO of the PDF from S3
        """
        response = self.s3_client.get_object(
            Bucket=bucket_name,
            Key=s3_key
        )
        return BytesIO(
            response['Body'].read()
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
        return self.s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=pdf_buffer.getvalue()
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
        with tempfile.NamedTemporaryFile(delete=True) as temp_input, \
                tempfile.NamedTemporaryFile(delete=True) as temp_output:
            temp_input.write(
                template_buffer.read()
            )
            temp_input.flush()
            fillpdfs.write_fillable_pdf(
                temp_input.name,
                temp_output.name,
                data,
                flatten=annot_flatten
            )
            temp_output.seek(0)
            output_buffer = BytesIO(temp_output.read())
            return output_buffer

    def flatten_pdf_buffer_as_images(
        self,
        input_buffer: BytesIO,
        retain_x_scale: bool = True,
    ) -> BytesIO:
        """
        Flattens the input_buffer PDF as images
        Returns BytesIO of the flattened PDF
        """
        with tempfile.NamedTemporaryFile(delete=True) as temp_input, \
                tempfile.NamedTemporaryFile(delete=True) as temp_output:
            temp_input.write(
                input_buffer.read()
            )
            temp_input.flush()

            fillpdfs.flatten_pdf(
                temp_input.name, temp_output.name, as_images=True)

            output_buffer = BytesIO()
            if retain_x_scale:
                original_pdf = pypdf.PdfReader(temp_input.name)
                original_page = original_pdf.pages[0]
                original_width = float(original_page.mediabox.width)
                flattened_pdf = pypdf.PdfReader(temp_output.name)
                output_pdf = pypdf.PdfWriter()
                for page in flattened_pdf.pages:
                    current_width = float(page.mediabox.width)
                    scale_x = original_width / current_width
                    page.scale_by(scale_x)
                    output_pdf.add_page(page)
                output_pdf.write(output_buffer)
                output_buffer.seek(0)
                return output_buffer
            temp_output.seek(0)
            output_buffer.write(temp_output.read())
            return output_buffer

    def add_blank_page(
            self,
            input_buffer: BytesIO,
    ) -> BytesIO:
        """
        Adds a blank page to the end of input_buffer PDF
        Returns BytesIO of the PDF with a blank page
        """
        reader = pdfrw.PdfReader(
            fdata=input_buffer.getvalue()
        )
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
        output_buffer = BytesIO()
        writer = pdfrw.PdfWriter()
        for page in reader.pages:
            writer.addpage(page)
        writer.addpage(blank_page)
        writer.write(output_buffer)
        output_buffer.seek(0)
        return output_buffer

    def merge_pdfs(
        self,
        pdf_buffers: list[BytesIO],
    ) -> BytesIO:
        """
        Merges list of PDFs into a single file
        Returns BytesIO of the merged PDF
        """
        writer = pdfrw.PdfWriter()
        for buffer in pdf_buffers:
            reader = pdfrw.PdfReader(
                fdata=buffer.getvalue()
            )
            for page in reader.pages:
                writer.addpage(page)
        output_buffer = BytesIO()
        writer.write(output_buffer)
        output_buffer.seek(0)
        return output_buffer

    def make_copies(
            self,
            pdf_buffer: BytesIO,
            copies: int,
    ) -> BytesIO:
        """
        Makes copies in order of the input PDF
        Returns BytesIO of the PDF with copies
        """
        reader = pdfrw.PdfReader(
            fdata=pdf_buffer.getvalue()
        )
        writer = pdfrw.PdfWriter()
        for _ in range(copies):
            writer.addpages(reader.pages)
        output_buffer = BytesIO()
        writer.write(output_buffer)
        output_buffer.seek(0)
        return output_buffer
