import boto3
from pathlib import Path



def is_not_edf(file_name: str) -> bool:
    return not file_name.endswith('.edf')

class Tracker:
    def __init__(self, fp: Path, name: str):
        self.fp: Path = fp
        self.counter: int = -1
        self.name: str = name
        self.recover()

    def recover(self):
        if self.fp.is_file():
            with open(self.fp, 'r') as f:
                records = f.readlines()
            self.counter = int(records[-1][0]) + 1
            print(f'[{self.name}] recover {self.counter - 1}')
        else:
            self.counter = 0

    def update(self, msg=None):
        print(f'[{self.name}] update {self.counter}')
        with open(self.fp, 'a') as f:
            _msg = f', {msg}' if msg is not None else ''
            f.write(f'{self.counter}{_msg}\n')
        self.counter += 1

    def clear(self):
        print(f'[{self.name}] clear')
        self.fp.unlink(missing_ok=True)
        self.counter = 0

    def get_status(self) -> int:
        return self.counter


class Downloader:
    def __init__(self, arn: str, local_dir: Path):
        self.arn = arn
        self.local_dir = local_dir
        self.s3 = boto3.resource('s3')
        self.bdsp_bucket = self.s3.Bucket(arn)
        self.dir_list_fp = self.local_dir / 'dir_list.txt'
        self.file_list_fp = self.local_dir / 'file_list.txt'


    def download_file_list(self):
        self.dir_list_fp.unlink(missing_ok=True)
        self.file_list_fp.unlink(missing_ok=True)

        dir_counter = 0
        file_counter = 0
        for objects in self.bdsp_bucket.objects.filter(Prefix="EEG/"):
            obj_key = objects.key
            print(obj_key)
            if obj_key.endswith("/"):
                # if it is directory, append to dir_list and increment the counter
                with open(self.dir_list_fp, 'a') as df:
                    df.write(f'{dir_counter}, {obj_key}\n')
                    dir_counter += 1

            else:
                with open(self.file_list_fp, 'a') as ff:
                    ff.write(f'{file_counter}, {obj_key}\n')
                    file_counter += 1

    def mkdirs(self, restart=False):
        tracker = Tracker(self.local_dir / 'mkdir.log', name='mkdir')
        if restart:
            tracker.clear()

        with open(self.dir_list_fp, 'r') as f:
            directories = f.readlines()[tracker.get_status():]

        for directory in directories:
            dir_name = directory.strip().split(', ')[1]
            local_directory = self.local_dir / dir_name
            local_directory.mkdir(parents=True, exist_ok=True)
            tracker.update(msg=dir_name)


    def download_data(self, filter=None, restart=False):
        tracker = Tracker(self.local_dir / 'download.log', name='download')

        if restart:
            tracker.clear()

        with open(self.file_list_fp, 'r') as f:
            files = f.readlines()[tracker.get_status():]

        for file in files:
            file_name = file.strip().split(', ')[1]
            local_file = self.local_dir / file_name

            if filter is not None:
                if not filter(file_name):
                    print(f'skip {file_name}')
                    tracker.update('skip')
                    continue

            with open(local_file, 'wb') as f:
                print(f'downloading {file_name} to {local_file}')
                self.s3.download_fileobj(self.arn, file_name, f)
                tracker.update(file_name)



def get_file_list(local_dir, arn):

    s3 = boto3.resource('s3')
    bdsp_bucket = s3.Bucket(arn)

    dir_list_fp = local_dir / 'dir_list.txt'
    file_list_fp = local_dir / 'file_list.txt'

    dir_list_fp.unlink(missing_ok=True)
    file_list_fp.unlink(missing_ok=True)

    dir_counter = 0
    file_counter = 0
    for objects in bdsp_bucket.objects.filter(Prefix="EEG/"):
        obj_key = objects.key
        print(obj_key)
        if obj_key.endswith("/"):
            # if it is directory, append to dir_list and increment the counter
            with open(dir_list_fp, 'a') as df:
                df.write(f'{dir_counter}, {obj_key}\n')
                dir_counter += 1

        else:
            with open(file_list_fp, 'a') as ff:
                ff.write(f'{file_counter}, {obj_key}\n')
                file_counter += 1


if __name__ == "__main__":
    local_dir = Path('/Volumes/EEG/harvard_eeg_db_v2')
    ap_arn = 'arn:aws:s3:us-east-1:184438910517:accesspoint/bdsp-eeg-access-point'

    # get_file_list(local_dir, ap_arn)

    downloader = Downloader(ap_arn, local_dir)
    downloader.mkdirs()
    downloader.download_data(filter=is_not_edf)

