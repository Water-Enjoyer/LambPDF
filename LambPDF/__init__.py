import os
lamb_local = os.getenv("LAMB_LOCAL")
if lamb_local:
    FONTCONFIG_PATH = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "opt",
        "fonts",
    )
else:
    FONTCONFIG_PATH = "/opt/fonts"

os.environ["FONTCONFIG_PATH"] = FONTCONFIG_PATH
from LambPDF.LambPDF import LambPDF as LambPDF  # noqa
