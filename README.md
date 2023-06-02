## Environment setup for TIFF compression script

You need to setup a python environment which is easiest archied by using Anaconda.

You should create conda environment using the following command:
```bash
conda create -y -n napari-env -c conda-forge python=3.9 
conda activate napari-env
conda install -c conda-forge napari
pip install gooey
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
python compress_tiffs.py
```

If you want, you can use command line version as such:
```bash
cd <folder with compress_tiffs_cli.py script>
python compress_tiffs_cli.py -d <your directory with images to compress> --compression jpeg_2000_lossy --quality 85 --cache_dir <location of your cache dir>
```

Using `cache_dir` parameter is strongly recommended when saving to a network attached storage.
