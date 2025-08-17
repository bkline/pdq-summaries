# PDQ Summary Publishing

This repository contains a port of the software which published PDQ summaries to the NCI web site, without any dependencies on the CDR (the original system which provided that functionality). The documents which are the source for those summaries are stored in this repository alongside the source code for the publishing software which transforms those documents and pushes them to the Drupal CMS. A full load of all 1,330 summaries to a local Docker instance of the CMS takes about 20 minutes.

## Runtime Options

```
$ ./publish.py --help
usage: publish.py [-h] [--base BASE] [--batch BATCH] [--debug] [--dump] [--ids IDS [IDS ...]]
                  [--max MAX] [--skip SKIP] [--tier TIER] [--type {cis,dis}]

options:
  -h, --help           show this help message and exit
  --base BASE          base URL for CMS (default: http://www.devbox)
  --batch BATCH        number to mark as publishable in each call
  --debug              enable debug logging
  --dump               store summary JSON locally instead of pushing it
  --ids IDS [IDS ...]  push specific summaries
  --max MAX            maximum number of summaries to push
  --skip SKIP          number of summaries to skip past
  --tier TIER          where to link for media on Akamai
  --type {cis,dis}     restrict push to single summary type
  ```

Most of the options can be intuitively understood from the brief descriptions in the usage statement shown above.

There are several ways to specify which summaries should be processed. The most straightforward uses the `--ids` option to provide specific document IDs. If that option is not used, the software defaults to publishing all of the summaries. The `--type` option lets you specify "cis" to have only the Cancer Information Summary documents processed, or "dis" to select just the Drug Information Summary documents. In addition, you can process the documents in sub-batches using the `--skip` and `--max` options. This technique has sometimes been needed in the past when the Drupal server becomes overloaded.

If you include the `--dump` option the software will write the generated JSON to local files instead of pushing the values to the CMS server. This takes about five seconds for the entire set of all the summaries.

## TODO

A possible future enhancement would be to determine for each summary document whether what we have in the repository has changed since the last time it was pushed to the CMS. One approach for doing this would be to remember what we sent the last time for each document and compare what we generate during the current job to find out if it has changed. There are a couple of drawbacks to this technique, which was basically how the original CDR system determined whether to optimize away the push of each document. For one thing, while it works pretty well on the production tier, which is relatively stable, it is easily confused when pushing to non-production servers, which are much more volatile. In addition, even on the production server the summary documents can be modified manually by users, which this technique would not detect.

Another approach would be to ask the Drupal server to send a copy of what it already has for the summary and compare it with what we are currently generating, and only push the document if they differ (or if the CMS doesn't have the document at all). This method would avoid the drawbacks described for the first approach described above, and would probably introduce no more complexity (and possibly less). However, it would not be as efficient as the first approach. Of course, some of that efficiency loss would be offset by optimization introduced by avoiding unnecessary document pushes, as well as the processing sweep to move the newly pushed documents from the draft to the published state. It would also reduce the number of revisions created on the Drupal server, both for the nodes and for the paragraph entities.