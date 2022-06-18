import pydra
import nipype.interfaces.fsl as fsl
from nipype.interfaces.ants.segmentation import DenoiseImage

from nipype.interfaces.io import BIDSDataGrabber

from pathlib import Path, PurePath


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
                            args = "40 174 36 230 165 145",
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

# ############################################################################

def build_output_workflow(wf_name: str = "output_pipe") -> Workflow:
    """Example of an output workflow.

    :param name: The name of the workflow.
    :return: The output workflow.
    """
    import pydra

    @pydra.mark.task
    @pydra.mark.annotate({"return": {"output_file": str}})
    def bids_writer_task(input_file: PurePath, output_dir: PurePath):
        """
        Task to write files to output_dir
        """
        import subprocess
        output_file = output_dir / PurePath(input_file).name
        print(f"{output_file}")

        return output_file

    wf = Workflow(name=wf_name, input_spec=["input_file", "output_dir"])
    wf.add(bids_writer_task(name="bids_writer", input_file=wf.lzin.input_file, output_dir=wf.lzin.output_dir))

    wf.set_output([("output_file", wf.bids_writer.lzout.output_file)])

    return wf



# ############################################################################ definition of the main workflow

#test_data_dir = Path("/home/INT/meunier.d/Data/export_XNAT/XNAT-PNH_ANAT_CHAVA-2021-11-05_10h15")

test_data_dir = Path("/envau/work/crise/meunier.d/Data/Data_Laura_Lopez/")

workflow = Workflow(
    name="test_data_prep_wf",
    input_spec=["input_dir", "output_dir"],
    input_dir=test_data_dir,
    output_dir=test_data_dir  / "test_data_prep_wf",
)

wf_input = create_import_pipe()
wf_input.inputs.input_dir = workflow.lzin.input_dir
workflow.add(wf_input)

wf_core = create_short_preparation_pipe()
wf_core.inputs.t1w_file = wf_input.lzout.t1w_files
workflow.add(wf_core.split("t1w_file"))

wf_output = build_output_workflow()
wf_output.inputs.input_file = wf_core.lzout.t1w_cropped_denoised_file
wf_output.inputs.output_dir = test_data_dir / "test_data_prep_wf"

workflow.add(wf_output)

workflow.set_output([
    ("t1w_cropped_file", wf_core.lzout.t1w_cropped_file),
    ("t1w_cropped_denoised_file", wf_output.lzout.output_file)
    ])

### Adding run
from pydra import Submitter

with Submitter(plugin="cf", n_procs=4) as submitter:
    submitter(workflow)

results = workflow.result(return_inputs=True)

print(results)
