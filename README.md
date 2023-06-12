## Environment setup for TIFF compression script

You need to setup a python environment which is easiest archied by using mamba.
You should create conda environment using the following commands.
Open miniforge Powershell Prompt (on Windows) (or equivalent) and type the following:
```bash
mamba create -y -n tiff_compression -c conda-forge python=3.11 
mamba activate tiff_compression
pip install numpy, tqdm, tifffile, gooey
```

this will install all necessary packages for the compression script.

You can run the TIFF compression script in the same prompt such as:
```bash
cd <folder with full_caching_compress_tiffs.py script>
python full_caching_compress_tiffs.py
```

Using `cache_dir` is required for the `full_caching_compress_tiffs.py` script, by default up to 3 files at a time will be temporarily copied to the directory from where you launch the script. Alternatevely the directory in the `cache_dir` paramtere will be used.

Additionally you may want to create an environment with Napari to quickly open compressed images:
```bash
mamba create -y -n devbio_napari_env -c conda-forge python=3.9 
mamba activate napari-env
mamba install -c conda-forge napari
```
To launch Napari, 
```bash
mamba activate devbio_napari_env
napari
```