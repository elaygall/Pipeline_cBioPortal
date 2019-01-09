import requests
import luigi
import pandas as pd
import os
from luigi.contrib.external_program import ExternalProgramTask

CANCER_TYPE = 'brca_tcga' #This constant is the ID name of dataset from cBioPortal. Change it if you need to check another one.


class DownloadMutList(luigi.Task):

    """ Download MAF for our use. """

    def requires(self):
        return []

    def run(self):
        hp = 'https://media.githubusercontent.com/media/cBioPortal/datahub/master/public/'
        path = str(os.getcwd()) + '/' + CANCER_TYPE
        os.mkdir(path)
        r = requests.get(hp + "{}/data_mutations_extended.txt".format(CANCER_TYPE),
                         stream=True)
        maf = r.text.encode('UTF-8')
        with self.output().open('w') as f:
            f.write(maf)

    def output(self):
        return luigi.LocalTarget("{}/data_mutations.csv".format(CANCER_TYPE))


class CreateMutsigFile(luigi.Task):

    """ Create list of genes with MutSig2 annotation from cBioPortal. """

    def requires(self):
        return [DownloadMutList()]

    def run(self):
        df = pd.read_csv("{}/data_mutations.csv".format(CANCER_TYPE),
                         sep='\t', dtype={"Start_Position": int, "End_Position": int})
        filtered = df[['Hugo_Symbol', 'Entrez_Gene_Id', 'MUTSIG_Published_Results']]
        filtered = filtered.dropna()
        filtered.to_csv('{}/dme_filtered_mutsig.csv'.format(CANCER_TYPE),
                        sep='\t', encoding='utf-8', index=False)

    def output(self):
        return luigi.LocalTarget("{}/dme_filtered_mutsig.csv".format(CANCER_TYPE))


class CreateFileSM(luigi.Task):

    """ Create file with silent mutations for oncodriveCLAST. """

    def requires(self):
        return [CreateMutsigFile()]

    def run(self):
        df = pd.read_csv("{}/data_mutations.csv".format(CANCER_TYPE),
                         sep='\t', dtype={"Start_Position": int, "End_Position": int})
        dfsm = df.loc[df['Variant_Classification'] == 'Silent']
        dfsm = dfsm[['Hugo_Symbol', 'Protein_position']]
        dfsm.to_csv('{}/dme_silent_mutations.txt'.format(CANCER_TYPE),
                    sep='\t', encoding='utf-8', index=False)

    def output(self):
        return luigi.LocalTarget("{}/dme_silent_mutations.txt".format(CANCER_TYPE))


class CreateFileNotSM(luigi.Task):
    """Create file with the protein affecting mutations for oncodriveCLAST."""

    def requires(self):
        return [CreateFileSM()]

    def run(self):
        df = pd.read_csv("{}/data_mutations.csv".format(CANCER_TYPE),
                         sep='\t', dtype={"Start_Position": int, "End_Position": int})
        mut_list = ['Missense_Mutation', 'Nonsense_Mutation']
        dfmap = df.loc[df['Variant_Classification'].isin(mut_list)]
        dfmap = dfmap[['Hugo_Symbol', 'Protein_position']]
        dfmap = dfmap.dropna()
        dfmap.to_csv('{}/dme_protafc_mutations.txt'.format(CANCER_TYPE),
                     sep='\t', encoding='utf-8', index=False)

    def output(self):
        return luigi.LocalTarget("{}/dme_protafc_mutations.txt".format(CANCER_TYPE))


class OncodriveResult(luigi.contrib.external_program.ExternalProgramTask):

    """Run oncodriveCLAST for the selected dataset and get the result in *.tsv format. """

    def requires(self):
        return [CreateFileNotSM()]

    def program_args(self):
        return ["oncodriveclust", "-m", "3", "--cgc", "data/CGC_phenotype.tsv",
                "{}/dme_protafc_mutations.txt".format(CANCER_TYPE),
                "{}/dme_silent_mutations.txt".format(CANCER_TYPE),
                "data/gene_transcripts.tsv", "--out",
                "{}/oncodriveclust-results.tsv".format(CANCER_TYPE)]

    def output(self):
        return luigi.LocalTarget("{}/oncodriveclust-results.tsv".format(CANCER_TYPE))


class FilterOncodriveRes(luigi.Task):

    """ Create *.csv file from oncodrive results without NaN. """

    def requires(self):
        return [OncodriveResult()]

    def run(self):
        df = pd.read_csv("{}/oncodriveclust-results.tsv".format(CANCER_TYPE),
                         sep='\t')
        df = df.drop('CGC', 1)
        df = df.dropna()
        df.to_csv('{}/odc_res.csv'.format(CANCER_TYPE),
                        sep='\t', encoding='utf-8', index=False)

    def output(self):
        return luigi.LocalTarget('{}/odc_res.csv'.format(CANCER_TYPE))


if __name__ == '__main__':
    luigi.run()
