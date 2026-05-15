# media-manager-movies

Movie media manager for MuxCore. Handles movie requests, indexer search, and download coordination.

## Usage

```go
import "github.com/Muxcore-Media/media-manager-movies"

mod := movies.NewModule(reg, bus)
mgr.Register(mod, nil)
```

The module listens for `media.requested` events with `media_type: "movie"` and orchestrates search → download.
