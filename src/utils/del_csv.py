import os
import glob

def delete_all_csvs(directory):
    files = glob.glob(f'{directory}/**/*.csv', recursive=True)
    for file in files:
        try:
            os.remove(file)
            print(f'{file} has been deleted.')
        except OSError as e:
            print(f'Error: {file} : {e.strerror}')

def main():
    delete_all_csvs('data')

if __name__ == "__main__":
    main()
