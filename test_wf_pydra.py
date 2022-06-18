import pydra
import nipype.interfaces.fsl as fsl
from nipype.interfaces.ants.segmentation import DenoiseImage

from nipype.interfaces.io import BIDSDataGrabber

bids_data_grabber = BIDSDataGrabber(
    outfields=["T1w"],
    output_query={
        "T1w": {
            "datatype": "anat",
            "suffix": "T1w",
            "extension": [".nii.gz"],
        }
    }
)

from pydra import Workflow
from pydra.tasks.nipype1.utils import Nipype1Task

def create_import_pipe(name: str  = "import_pipe") -> Workflow:

    import_pipe = Workflow(name=name, input_spec=["input_dir"])

    import_pipe.add(
        Nipype1Task(
            name="bids_data_grabber",
            interface=bids_data_grabber,
            base_dir=import_pipe.lzin.input_dir,
        )
    )

    import_pipe.set_output([
        ("t1w_files", import_pipe.bids_data_grabber.lzout.T1w)
    ])

    return import_pipe

def create_short_preparation_pipe(name: str ="short_preparation_pipe") -> Workflow:

    """Description: short data preparation (average, reorient, crop/betcrop \
    and denoise)

    Processing steps;

    - Cropped (bet_crop include alignement, and mask generation)
    - denoising all images (no denoise_first)

    Inputs:

        inputnode:

            T1:
                T1 file (from BIDSDataGrabber)

        arguments:

            name:
                pipeline name (default = "long_multi_preparation_pipe")

    Outputs:

        outputnode:

            preproc_T1:
                preprocessed T1 file
    """

    # creating pipeline
    short_preparation_pipe = Workflow(name, str = name, input_spec = ["t1w_file"])

    # cropping
    # Crop bounding box for T1
    crop_T1 = Nipype1Task(fsl.ExtractROI(),
                            name='crop_T1',
                            args = "101 10 23 34 43 43",
                            in_file = short_preparation_pipe.lzin.t1w_file)

    short_preparation_pipe.add(crop_T1)

    # denoise with Ants package
    denoise_T1 = Nipype1Task(interface=DenoiseImage(),
                            input_image = short_preparation_pipe.crop_T1.lzout.roi_file,
                            name="denoise_T1")

    short_preparation_pipe.add(denoise_T1)

    short_preparation_pipe.set_output([
        ("t1w_cropped_file", short_preparation_pipe.crop_T1.lzout.roi_file),
        ("t1w_cropped_denoised_file", short_preparation_pipe.denoise_T1.lzout.output_image)
        ])
    return short_preparation_pipe

### definition of the main workflow

from pathlib import Path

#test_data_dir = Path("/home/INT/meunier.d/Data/export_XNAT/XNAT-PNH_ANAT_CHAVA-2021-11-05_10h15")

test_data_dir = Path("/envau/work/crise/meunier.d/Data/Data_Laura_Lopez/")

workflow = Workflow(
    name="test_data_prep_wf",
    input_spec=["input_dir", "output_dir"],
    input_dir=test_data_dir,
    output_dir=test_data_dir / "test_pydra_wf",
)

wf_input = create_import_pipe()
wf_input.inputs.input_dir = workflow.lzin.input_dir
workflow.add(wf_input)

wf_core = create_short_preparation_pipe()
wf_core.inputs.t1w_file = wf_input.lzout.t1w_files
workflow.add(wf_core.split("t1w_file"))

workflow.set_output([
    ("t1w_cropped_file", wf_core.lzout.t1w_cropped_file),
    ("t1w_cropped_denoised_file", wf_core.lzout.t1w_cropped_denoised_file)
    ])

### Adding run
from pydra import Submitter

with Submitter(plugin="cf", n_procs=4) as submitter:
    submitter(workflow)

results = workflow.result(return_inputs=True)

print(results)
