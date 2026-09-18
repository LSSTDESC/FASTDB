import { fastdbap } from "./fastdb_ns.js"
import "./fastdb.js"

// **********************************************************************
// **********************************************************************
// **********************************************************************
// Here is the thing that will make the code run when the document has loaded

fastdbap.started = false

// console.log("About to window.setInterval...");
fastdbap.init_interval = window.setInterval(
    function()
    {
        var requestdata, renderer
        
        if (document.readyState == "complete")
        {
            // console.log( "document.readyState is complete" );
            if ( !fastdbap.started )
            {
                fastdbap.started = true;
                window.clearInterval( fastdbap.init_interval );
                renderer = new fastdbap.Context();
                renderer.init();
            }
        }
    },
    100);

export { }
