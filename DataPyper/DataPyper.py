import os
import numpy as np
import pandas as pd

from nipype.interfaces.base import TraitedSpec, BaseInterface, Bunch
from nipype.interfaces.traits_extension import traits # , File
import nipype.pipeline.engine as pe


class CSVFileInputSpec(TraitedSpec):                                                                                                 
    csv_filepath = traits.File(mandatory = True, desc = "path to input csv file")
    rename_header = traits.Dict(desc = """
    dictionary of the form

    headername : newheadername
    """)

class CSVFileOutputSpec(TraitedSpec):
    """
    should be list of Bunch objects
    
    """
    data_frame = traits.Any()

class CSVFile(BaseInterface):

    input_spec = CSVFileInputSpec
    output_spec = CSVFileOutputSpec

    def _run_interface(self, runtime):
        runtime.returncode = 0
        return runtime

    def __init__(self, **inputs):
        super(CSVFile, self).__init__(**inputs)

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['data_frame'] = self._data_frame
        return outputs

    def _run_interface(self, runtime):
        cwd = os.getcwd()
        df = pd \
                .read_csv(self.inputs.csv_filepath) \
                .rename(columns = self.inputs.rename_header) \
                .sort('onset')

        ## derive a duration column if it doesn't exist
        if (df.columns == "duration").sum() == 0:
            df['duration'] = df['onset'].diff().shift(-1)

        ## FIXME
        ## stuff the last value with the mean duration
        ## there isn't any empirical reason why this is
        ## a good idea
        df['duration'][-1:] = df['duration'].mean()
        self._data_frame = df
        runtime.returncode = 0
        return runtime


class DataSelectorInputSpec(TraitedSpec):                                                                                                 
    data_frame = traits.Any() # should be a pandas DataFrame
    condition_definition = traits.List()

class DataSelectorOutputSpec(TraitedSpec):
    """
    should be list of Bunch objects
    
    """
    data_frame = traits.Any()

class DataSelector(BaseInterface):

    input_spec = DataSelectorInputSpec
    output_spec = DataSelectorOutputSpec

    def _run_interface(self, runtime):
        runtime.returncode = 0
        return runtime

    def __init__(self, **inputs):
        super(DataSelector, self).__init__(**inputs)

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['data_frame'] = self._data_frame
        return outputs

    def _k2var(self, k):
        # return "col_%s" % k
        return k

    def _run_interface(self, runtime):
        ENDTIME_COLNAME = 'xx_post_onset_termination'
        cwd = os.getcwd()
        df = self.inputs.data_frame

        df[ENDTIME_COLNAME] = df['onset'] + df['duration']
        for k in df.keys():
            if k.startswith("Unnamed"): continue
            varname = self._k2var(k)
            exec("%s = df['%s']" % (varname, k))

        dcond = {}
        dcount = {}
        row_sum = 0
        for conddef in self.inputs.condition_definition:
            condname, evalstr = map(str.strip, conddef.split('=', 1))
            dcond[condname] = df[eval(evalstr)]
            dcount[conddef] = dcond[condname].shape[0]
            row_sum += dcond[condname].shape[0]

        dfonset = pd.concat(dcond.values())

        ## FIXME
        ## make this more flexible
        ## essentially we want to sort from slow to fast
        ## on certain given columns
        ## i.e. run -> trial -> onset
        ## because a similar onset may appear over different runs
        lsksort = []
        if "run_number" in df.keys():
            lsksort.append("run_number")
        if "trial_number" in df.keys():
            lsksort.append("trial_number")
        lsksort.append("onset")

        ## FIXME:
        ## the overlap check tolerance below should be configurable
        ## check for time overlap (some other event happens during another event)
        ## naive test of whether any onset begins before the previous row's end time
        dfonset_sorted = dfonset.sort(lsksort)
        idx_is_overlap = dfonset_sorted.index[~(dfonset_sorted[ENDTIME_COLNAME][:-1] <= (dfonset_sorted['onset'][1:] + 1.2))]
        if idx_is_overlap.shape:
            sidx_is_overlap = idx_is_overlap.to_series()
            sidx_is_overlap = sidx_is_overlap.append(sidx_is_overlap - 1)
            sidx_is_overlap.sort()
            ## get the accused rows and the rows following those
            print("""
            * * * WARNING: * * *

            There are overlaps in your event specification!

            \n%s
            """ % (dfonset_sorted.ix[sidx_is_overlap]))

        ## check for duplicate onsets
        idx_duplicated_onset = dfonset.duplicated('onset')
        if idx_duplicated_onset.sum() > 0:
            v_duplicated_onset = dfonset[idx_duplicated_onset]['onset']
            col_display = [k for k in dfonset.keys() if k != ENDTIME_COLNAME]
            raise Exception("""
                    Overlapping events found!

                    These onsets are duplicated in your event specification:

                    \n%s
            
            """ % (dfonset[dfonset['onset'].isin(v_duplicated_onset)][col_display]))

        ## check for over-specified model
        if not row_sum < df.shape[0]:
            raise Exception("""
                    There are more events in your specification
                    than there are total rows in the data!
                    You specified : %s
                    Total rows    : %s

                    The conditions you specified:
                    =============================
                    %s
                    """ % (row_sum, df.shape[0], str("\n"+(" " * 20)).join(["(%s) %s"%(str(count).rjust(3), conddef) for conddef,count in dcount.items()])))

        self._data_frame = self.inputs.data_frame
        runtime.returncode = 0
        return runtime


if __name__ == "__main__":
    

    from pandas import DataFrame as DF

    try:
        ## generate some random csv datastructure
        import faker, random, tempfile
        FK = faker.Faker()
        ncol = random.randint(1, 5)
        nrow = random.randint(100, 400)

        df = DF(dict([(key, np.random.rand(1, nrow)[0]) for key in [FK.username() for i in range(ncol-1)] + ["RT"]]))
        for k in df.keys():
            if k == "RT": continue
            if random.random() > 0.5:
                ## turn the column into a binary value
                df[k] = df[k].round()

        TR_duration = 0.5 * random.randint(2, 6)
        ## append a duration and onset
        df['duration'] = TR_duration
        df['onset'] = df.index * TR_duration

        csv_filepath = tempfile.mktemp(suffix = ".csv")
        df.to_csv(csv_filepath)

        csvf = pe.Node(name = "csvfile", interface = CSVFile())
        csvf.inputs.csv_filepath = csv_filepath
        # res = csvf.run()

        ds = pe.Node(name = "dataselector", interface = DataSelector())
        ds.inputs.condition_definition = [
                "run1 = onset >= 10",
                "run2 = (onset > 20) & (onset < 30)",
                
                ]

        wf = pe.Workflow(name="wf")
        wf.base_dir = "/tmp"
        wf.config['crashdump_dir'] = "/tmp/crashdump"
        wf.connect([
            (csvf, ds, [("data_frame", "data_frame")]),
            ])

        res = wf.run()

        os.unlink(csv_filepath)

    except Exception, e:
        raise e


