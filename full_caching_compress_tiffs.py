from datetime import datetime
import os
import argparse
import shutil
import time
import pkg_resources
import tifffile
from tqdm import tqdm
import threading
import queue
import logging
from gooey import Gooey

MAX_FILES_IN_CACHE = 3
COMPRESSION_RATIO_THRESHOLD = 1.5
PROCESSING_THEAD_TIMEOUT = 100
COMPRESSED_FILES_FILE = "_already_compressed_files"
COMPRESSED_FOLDER = "_compressed_files"


def logging_broadcast(string):
	print(string)
	logging.info(string)

def copy_files_to_cache(remote_files, cache_dir, cache_queue, semaphore):
    for remote_file_path in remote_files:
        cache_file_path = os.path.join(cache_dir, os.path.basename(remote_file_path))

        # Acquire the semaphore before adding the local file path to the cache queue
        semaphore.acquire()
        try:
            # Download the file from the remote location to the local cache folder
            shutil.copy2(remote_file_path, cache_file_path)
            # Add the local file path to the cache queue
            cache_queue.put((cache_file_path, remote_file_path))
            logging_broadcast(f"Cached file: {cache_file_path}")
        except Exception as e:
            logging_broadcast(f"ERROR: Failed to cache the file. {e}")


def compress_one_file(
        remote_file_paths, pbar, cache_queue: queue.Queue, semaphore, done_paths_file, quality, compression, threads,
        replace_files,):

    processed_files = 0
    timeout = PROCESSING_THEAD_TIMEOUT
    while processed_files < len(remote_file_paths):
        if cache_queue.empty():
            time.sleep(1)
            timeout -= 1
            if timeout < 0:
                logging_broadcast("FATAL ERROR: Processing thread reached a timeout!")
                exit(1)
            continue
        timeout = PROCESSING_THEAD_TIMEOUT
        
        cached_file_path, remote_file_path = cache_queue.get()
        temp_cached_file_path = cached_file_path + '.part'
        remote_dir_with_file = os.path.dirname(remote_file_path)
        if not replace_files:
            compressed_dir = os.path.join(remote_dir_with_file, COMPRESSED_FOLDER)
            os.makedirs(compressed_dir, exist_ok=True)
            temp_remote_file_path = os.path.join(compressed_dir, os.path.basename(remote_file_path))
        else:
            temp_remote_file_path = remote_file_path + '.part'

        error_compressing = False
        try:
            original_file_size = os.path.getsize(cached_file_path)
            # Compress the TIFF file using the specified algorithm and quality
            tifffile_version = pkg_resources.get_distribution("tifffile").version
            with tifffile.TiffWriter(temp_cached_file_path) as tiff:
                if tifffile_version > "2022.7.28":
                    if compression == "jpeg_2000_lossy":
                        tiff.write(tifffile.imread(cached_file_path), compression=compression, compressionargs={'level': quality}, maxworkers=threads)
                    else:
                        tiff.write(tifffile.imread(cached_file_path), compression=compression, maxworkers=threads)
                else:
                    tiff.write(tifffile.imread(cached_file_path), compression=(compression, quality), maxworkers=threads)
        except Exception as e:
            logging_broadcast(f"Error compressing: {remote_file_path}")
            logging_broadcast(e)
            error_compressing = True          
            if os.path.exists(temp_cached_file_path):
                os.remove(temp_cached_file_path)  
            cache_queue.task_done()
            pbar.update(1)
            logging_broadcast("")
            semaphore.release()
            processed_files += 1
            continue

        os.remove(cached_file_path)
        compressed_file_size = os.path.getsize(temp_cached_file_path)
        compression_ratio =  float(original_file_size) / compressed_file_size


        if compression_ratio > COMPRESSION_RATIO_THRESHOLD:
            try:
                # Move the compressed file from the temporary cache directory to the final destination
                shutil.move(temp_cached_file_path, temp_remote_file_path)
            except OSError as e:
                # That is a workaround when shutil is raising an error when copying file to samba share where you can't copy permissions
                if e.errno == 95:
                    if os.path.isfile(temp_cached_file_path):
                        os.remove(temp_cached_file_path)
                else:
                    logging_broadcast(f"Error compressing: {remote_file_path}\n" + str(e))
                    error_compressing = True
            if replace_files and error_compressing == False:
                # Replace the original file with the compressed file
                try:
                    shutil.move(temp_remote_file_path, remote_file_path)
                except OSError as e:
                    if e.errno == 95:
                        pass
                    else:
                        logging_broadcast(f"Error compressing: {remote_file_path}\n" + str(e))
                        error_compressing = True
            logging_broadcast(f"Compressed: {remote_file_path}, compression ratio: {round(compression_ratio, 2)}x")
        else:
            logging_broadcast(f"Compression ratio is below {COMPRESSION_RATIO_THRESHOLD}, skipping file {remote_file_path}")
            os.remove(temp_cached_file_path)
        
        if error_compressing == False:
            with open(done_paths_file, 'a') as f:
                f.write(remote_file_path + "\n")
            
        # Release the semaphore after the compressed file has been copied to the remote location

        # Mark the file task as done in the cache queue
        cache_queue.task_done()
        pbar.update(1)
        logging_broadcast("")
        semaphore.release()
        processed_files += 1


def compress_tiff_files(input_path, cache_dir, *args):
    """
    Compresses the TIFF files in the given input path (folder or file) using the specified compression algorithm and quality percentage.
    Creates a new compressed TIFF file with the name '.part' and replaces the original file(s).
    """

    if not os.path.isdir(input_path):
        # Input path is a file
        file_path = input_path
        with tqdm(total=1, ncols=80, desc="Progress") as pbar:
            compress_one_file(file_path, pbar, *args)
        return True


    now = datetime.now()
    dt_string = now.strftime("%Y-%b-%d-%H%M%S")
    logging.basicConfig(filename=os.path.join(input_path, "%s-tiff_compression.log" % dt_string),
					 filemode='w',
					 format='%(asctime)s-%(levelname)s - %(message)s',
					 datefmt='%d-%b-%y %H:%M:%S',
					 level=logging.INFO)

    # Create a cache queue for the local file paths
    cache_queue = queue.Queue()
    
    # Create a semaphore to control the cache size
    semaphore = threading.Semaphore(MAX_FILES_IN_CACHE)

    done_paths_file = os.path.join(input_path, COMPRESSED_FILES_FILE)
    already_compressed_files = set()
    if os.path.exists(done_paths_file):
        with open(done_paths_file, 'r') as file:
            for line in file:
                file_path = line.strip()
                already_compressed_files.add(file_path)
        if already_compressed_files:
            logging_broadcast(f"Skipping already compressed files listed in {done_paths_file}")
    else:
        with open(done_paths_file, 'w'):
            pass
                


    remote_file_paths = []
    for root, dirs, files in os.walk(input_path):
        if COMPRESSED_FOLDER in dirs:
            dirs.remove(COMPRESSED_FOLDER)
        for file in files:
            if file.endswith('.tiff') or file.endswith('.tif'):
                file_path = os.path.join(root, file)
                if file_path not in already_compressed_files:
                    remote_file_paths.append(file_path)

    with tqdm(total=len(remote_file_paths) + len(already_compressed_files), ncols=80, desc="Progress") as pbar:

        pbar.update(len(already_compressed_files))
        pbar.refresh()
        print("")

        # Copy files to the local cache buffer asynchronously
        copy_thread = threading.Thread(target=copy_files_to_cache, args=(remote_file_paths, cache_dir, cache_queue, semaphore))
        copy_thread.start()

        # Process files from the cache queue
        process_thread = threading.Thread(target=compress_one_file, args=(
            remote_file_paths, pbar, cache_queue, semaphore, done_paths_file, *args))
        process_thread.start()

        # Wait for all files to be processed
        cache_queue.join()

        # Wait for the threads to complete
        copy_thread.join()
        process_thread.join()


@Gooey
def main():
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
        default=".")
    parser.add_argument(
        '--do_not_replace',
        action="store_true",
        help="Replace files at the destination? If enabled places file with extension '.part' beside original.",
        default=False)

    args = parser.parse_args()

    if args.folder:
        compress_tiff_files(args.folder, args.cache_dir, args.quality, args.compression,
                            args.threads, not args.do_not_replace)
    elif args.file:
        compress_tiff_files(args.file, args.cache_dir, args.quality, args.compression,
                            args.threads, not args.do_not_replace)


if __name__ == '__main__':
    main()
