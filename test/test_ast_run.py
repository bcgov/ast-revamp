from dataclasses import dataclass
from pathlib import Path
import sys

# Use main scripts dir for the project path
current_script_path = Path(__file__).resolve().parents[1]
sys.path.append(str(current_script_path))

from prelim.AST_outline import ASTProcessor

@dataclass
class ASTParameters:
    """Data class to hold all tool parameters."""
    feature: Path
    crown_file_num: int
    disp_num: int
    parcel_num: int
    output_dir: Path


def execute():
    """The source code of the tool."""
    # Create the dataclass instance from parameters
    params = ASTParameters(
        feature=None,
        crown_file_num='5406682',
        disp_num=907109,
        parcel_num=None,
        output_dir=Path("\path\to\output\folder"),
    )

    # Pass the dataclass instance to the processing class
    processor = ASTProcessor(**vars(params))
    processor.main()

    return

execute()