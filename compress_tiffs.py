import os
import argparse
import shutil
from time import sleep
import tifffile
from tqdm import tqdm


def compress_tiff_files(input_path, quality, compression, threads, cache_dir, replace_files):
    """
    Compresses the TIFF files in the given input path (folder or file) using the specified compression algorithm and quality percentage.
    Creates a new compressed TIFF file with the name '.part' and replaces the original file(s).
    """
    if os.path.isdir(input_path):
        # Input path is a folder
        tiff_files = []
        for root, _, files in os.walk(input_path):
            for file in files:
                if file.endswith('.tiff') or file.endswith('.tif'):
                    tiff_files.append(os.path.join(root, file))

        with tqdm(total=len(tiff_files), ncols=80, desc="Progress") as pbar:
            for file_path in tiff_files:
                temp_file_path = file_path + '.part'
                temp_file_path_beside_original = temp_file_path
                try:
                    if cache_dir:
                        cache_file_path = os.path.join(cache_dir, os.path.basename(temp_file_path))
                        temp_file_path = cache_file_path

                    # Compress the TIFF file using the specified algorithm and quality
                    with tifffile.TiffWriter(temp_file_path) as tiff:
                        tiff.write(tifffile.imread(file_path), compression=(compression, quality), maxworkers=threads)

                    if cache_dir:
                        # Move the compressed file from the temporary cache directory to the final destination
                        shutil.move(temp_file_path, temp_file_path_beside_original)
                    if replace_files:
                        # Replace the original file with the compressed file
                        shutil.move(temp_file_path_beside_original, file_path)
                    pbar.update(1)
                    print(f"Compressed: {file_path}")
                except Exception as e:
                    print(f"Error compressing: {file_path}")
                    print(e)
    else:
        # Input path is a file
        file_path = input_path
        temp_file_path = file_path + '.part'
        temp_file_path_beside_original = temp_file_path
        

        try:
            if cache_dir:
                cache_file_path = os.path.join(cache_dir, os.path.basename(temp_file_path))
                temp_file_path = cache_file_path
            # Compress the TIFF file using the specified algorithm and quality
            with tifffile.TiffWriter(temp_file_path) as tiff:
                tiff.write(tifffile.imread(file_path), compression=(compression, quality), maxworkers=threads)

            if cache_dir:
                # Move the compressed file from the temporary cache directory to the final destination
                shutil.move(temp_file_path, temp_file_path_beside_original)
                
            if replace_files:
                # Replace the original file with the compressed file
                shutil.move(temp_file_path, file_path)
            print(f"Compressed: {file_path}")
        except Exception as e:
            print(f"Error compressing: {file_path}")
            print(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Compress TIFF files using different compression algorithms.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--folder', help="Path to the folder containing TIFF files.")
    group.add_argument('-f', '--file', help="Path to the TIFF file.")

    parser.add_argument(
        '-C',
        '--compression', 
        choices=['jpeg_2000_lossy', 'zlib', 'lzw'],
        default='jpeg_2000_lossy', 
        help="Compression algorithm (default: jpeg_2000_lossy)")
    parser.add_argument(
                        '-Q',
                        '--quality', 
                        type=int,
                        help="Compression quality percentage (0-100). Required for jpeg_2000_lossy compression.", 
                        default=85)
    parser.add_argument(
                        '--threads', 
                        type=int,
                        help="Maximum number of threads to use (should be less that CPU cores).", 
                        default=None)
    parser.add_argument(
                        '--cache_dir', 
                        type=str,
                        help="Temporary directory on a fast local SSD, for better writing performance to network drives.", 
                        default=None)
    parser.add_argument(
                        '--do_not_replace', 
                        action="store_true",
                        help="Replace files at the destination? If enabled places file with extension '.part' beside original.", 
                        default=False)

    args = parser.parse_args()

    if args.folder:
        compress_tiff_files(args.folder, args.quality, args.compression, args.threads, args.cache_dir, not args.do_not_replace)
    elif args.file:
        compress_tiff_files(args.file, args.quality, args.compression, args.threads, args.cache_dir, not args.do_not_replace)
