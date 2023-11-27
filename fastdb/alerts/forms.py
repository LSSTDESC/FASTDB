from django import forms


class SnapshotForm(forms.Form):
    snapshot_name = forms.CharField(label="Snapshot name", max_length=20)

class ProcessingVersionsForm(forms.Form):
    version = forms.CharField(label="Processing Version", max_length=20)
    validity_start = forms.DateTimeField(label="Valid Start Date")

class EditProcessingVersionsForm(forms.Form):
    version = forms.CharField(label="Processing Version", max_length=20)
    validity_start = forms.DateTimeField(label="Valid Start Date")
    validity_end = forms.DateTimeField(label="Valid End Date")


