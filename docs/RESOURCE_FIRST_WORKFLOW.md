## Resource-first Upload Workflow

In traditional CKAN, the dataset package upload workflow is as follows:

1. Enter package metadata
2. Upload resource/s
3. Check if the datapusher uploaded the dataset correctly.
   - With the Datapusher,this make take a while, and when it fails, it doesn't really give you
     actionable information on why it failed.
   - With xloader, its 10x faster. But then, that speed comes at the cost of all columns defined as text,
     and the Data Publisher will need to manually change the data types in the Data Dictionary and
     reload the data again.

In [TNRIS/TWDB's extensive user research](https://internetofwater.org/blog/building-the-texas-water-data-hub-from-the-ground-up/),
one of the key usability gaps they found with CKAN is this workflow. Why can't the data publisher
upload the primary resource first, before entering the metadata? And more importantly, why can't some of the metadata
be automatically inferred and populated based on the attributes of the dataset?

This is why qsv's speed is critical for a Resource-first upload workflow. By the time the data publisher
uploads the resource and starts populating the rest of the form a few seconds later, a lot of inferred metadata
(Data Dictionary for this initial version) should be available for pre-populating the rest of the form.

See this [discussion](https://github.com/ckan/ckan/discussions/6689) and this [issue](https://github.com/ckan/ideas/issues/150)
about the "Multi-pass Datapusher" from May 2015 for additional context.
