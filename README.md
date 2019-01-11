# spget

[![Build Status](https://travis-ci.org/mkmik/spget.png)](https://travis-ci.org/mkmik/spget)

spget (pronounced like spaghetti) is a simple download manager that downloads a large file in parallel.

Unlike other parallel downloaders this tries to be stream friendly (i.e. streamable parallel get), by splitting
the file in small chunks (default 100k) and enqueuing the fetches sequentially.

Downloaded chunks are stored in temporary files (called filename.chunk-<n>) and merged to the main output in order.
The temporary files are deleted only when successfully merged. This means that the output file will not contain holes.

spget is able to resume an interrupted download. Both the output file and the partially downloaded chunks will be reused.

## Install

```bash
go get github.com/mmikulicic/spget
```

## Usage

```bash
spget -n 32 -o out.mp4 http://host/file.mp4
```

## Notes

Omitting the `-o` param will stream to stdout which unfortunately doesn't handle resumes, so it's wiser to specify an output file.
The output file name will be used as a basename to store the downloaded but unmerged chunks.

It's possible to change the chunk size after resuming a download, but you might have to manually delete temporary chunk files based
on the previous chunking.

I made this tool to workaround by ISP's rate limiter while I was streaming my movies stored in my favourite cloud storage.
As an alternative I just realized that I can use UDP based openvpn as a workaround so I'm not sure I'll continue fixing this tool
even if I find some bugs while I'm using it. Nevertheless feel free to report issues and send a pull request.
