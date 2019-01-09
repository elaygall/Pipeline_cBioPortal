# Pipeline_cBioPortal

Данный пайплайн реализован с помощью библиотеки luigi. В константе CANCER_TYPE необходимо указать имя директории анализируемого датасета.
Директории расположены в репозитории cBioPortal: https://github.com/cBioPortal/datahub/tree/master/public

## Pipeline:
### DownloadMutList
- Скачивает MAF файл с аннотированными мутациями и создаёт директорию с названием, эквивалентным CANCER_TYPE

### CreateMutsigFile
- Отфильтровывает гены с аннотацией MutSig

### CreateFileSM
- Создаёт файл с silent mutations для oncodriveCLUST

### CreateFileNotSM
- Создаёт файл с missense и nonsense mutations для oncodriveCLUST

### OncodriveResult
- Запускает oncodriveCLUST передавая на вход необходимые параметры и записывает итоговый файл в директорию к остальным файлам

### FilterOncodriveRes
- Отфильтровывает NaN значения для результирующего файла oncodriveCLUST
