## Environment setup for TIFF compression script

You need to setup a python environment which is easiest archied by using Anaconda.

You should create conda environment using the following command:
```bash
conda create -n devbio_napari_env devbio-napari python=3.9 -c conda-forge 
```

this will install all necessary packages as well as Napari viewer where you can quickly view compressed TIFF files.

To launch Napari, open Anaconda Powershell Prompt (on Windows) and type the following:
```bash
conda activate devbio_napari_env
napari
```

You can run the TIFF compression script in the same prompt such as:
```bash
cd <folder with compress_tiffs.py script>
python compress_tiffs.py -d <your directory with images to compress> --compression jpeg_2000_lossy --quality 85 
```

You can run:
```
python compress_tiffs.py --help
```
to see avaliable options and parameters.
