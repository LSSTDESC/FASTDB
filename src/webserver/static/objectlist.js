import { fastdbap } from "./fastdb_ns.js"
import { rkWebUtil } from "./rkwebutil.js"

// **********************************************************************
// **********************************************************************
// **********************************************************************

fastdbap.ObjectList = class
{
    constructor( context, parentdiv )
    {
        this.context = context;
        this.topdiv = parentdiv;
    }

    render_page( data )
    {
        let self = this;
        let tr;

        // Calculate magnitudes assuming zp=31.4 (flux is nJy)
        data[ 'magmax' ] = []
        data[ 'magmaxerr' ] = []
        data[ 'maglast' ] = []
        data[ 'maglasterr' ] = []
        data[ 'magforcedlast' ] = []
        data[ 'magforcedlasterr' ] = []
        for ( let i in data.diaobjectid ) {
            if ( data.maxdetflux[i] > 0 ) {
                data.magmax.push( -2.5 * Math.log10( data.maxdetflux[i] ) + 31.4 );
                data.magmaxerr.push( 2.5 / Math.log(10.) * data.maxdetfluxerr[i] / data.maxdetflux[i] );
            } else {
                data.magmax.push( -99 );
                data.magmaxerr.push( -99 );
            }
            if ( data.lastdetflux[i] > 0 ) {
                data.maglast.push( -2.5 * Math.log10( data.lastdetflux[i] ) + 31.4 );
                data.maglasterr.push( 2.5 / Math.log(10.) * data.lastdetfluxerr[i] / data.lastdetflux[i] );
            } else {
                data.maglast.push( -99 );
                data.maglasterr.push( -99 );
            }
            if ( data.lastforcedflux[i] > 0 ) {
                data.magforcedlast.push( -2.5 * Math.log10( data.lastforcedflux[i] ) + 31.4 );
                data.magforcedlasterr.push( 2.5 / Math.log(10.) * data.lastforcedfluxerr[i] / data.lastforcedflux[i] );
            } else {
                data.magforcedlast.push( -99 );
                data.magforcedlasterr.push( -99 );
            }
        }

        let fields = [ 'diaobjectid',
                       'ra',
                       'dec',
                       'ndet',
                       'lastdetfluxmjd',
                       'lastdetfluxband',
                       'maglast',
                       'maglasterr',
                       'maxdetfluxmjd',
                       'maxdetfluxband',
                       'magmax',
                       'magmaxerr',
                       'lastforcedfluxmjd',
                       'lastforcedfluxband',
                       'magforcedlast',
                       'magforcedlasterr' ]
        let hdrs = { 'lastdetfluxmjd': 'mjd',
                     'lastdetfluxband': 'band',
                     'maglast': 'mag',
                     'maglasterr': 'dmag',
                     'maxdetfluxmjd': 'mjd',
                     'maxdetfluxband': 'band',
                     'magmax': 'mag',
                     'magmaxerr': 'dmag',
                     'lastforcedfluxmjd': 'mjd',
                     'lastforcedfluxband': 'band',
                     'magforcedlast': 'mag',
                     'magforcedlasterr': 'dmag' }


        let rowrenderer = function( data, fields, i ) {
            let tr, td;
            tr = rkWebUtil.elemaker( "tr", null );
            let args = {
                'diaobjectid':        [ "td", tr, { "text": data.diaobjectid[i],
                                                    "classes": [ "link" ],
                                                    "click": (e) => { self.show_object_info( data.diaobjectid[i] ); }
                                                  } ],
                'ra':                 [ "td", tr, { "text": data.ra[i].toFixed(5) } ],
                'dec':                [ "td", tr, { "text": data.dec[i].toFixed(5) } ],
                'ndet':               [ "td", tr, { "text": data.ndet[i].toString() } ],
                'lastdetfluxmjd':     [ "td", tr, { "text": data.lastdetfluxmjd[i].toFixed(2),
                                                    "classes": [ "borderleft" ] } ],
                'lastdetfluxband':    [ "td", tr, { "text": data.lastdetfluxband[i] } ],
                'maglast':            [ "td", tr, { "text": data.maglast[i].toFixed(2) } ],
                'maglasterr':         [ "td", tr, { "text": data.maglasterr[i].toFixed(2) } ],
                'maxdetfluxmjd':      [ "td", tr, { "text": data.maxdetfluxmjd[i].toFixed(2),
                                                    "classes": [ "borderleft" ] } ],
                'maxdetfluxband':     [ "td", tr, { "text": data.maxdetfluxband[i] } ],
                'magmax':             [ "td", tr, { "text": data.magmax[i].toFixed(2) } ],
                'magmaxerr':          [ "td", tr, { "text": data.magmaxerr[i].toFixed(2) } ],
                'lastforcedfluxmjd':  [ "td", tr, { "text": data.lastforcedfluxmjd[i].toFixed(2),
                                                    "classes": [ "borderleft" ] } ],
                'lastforcedfluxband': [ "td", tr, { "text": data.lastforcedfluxband[i] } ],
                'magforcedlast':      [ "td", tr, { "text": data.magforcedlast[i].toFixed(2) } ],
                'magforcedlasterr':   [ "td", tr, { "text": data.magforcedlasterr[i].toFixed(2) } ]
            }
            for ( let f of fields ) {
                if ( ! args.hasOwnProperty( f ) ) {
                    console.log( "ERROR: unknown field " + f + "; you should never see this!" )
                    continue;
                }
                td = rkWebUtil.elemaker.apply( null, args[f] );
            }
            return tr;
        }

        let headercallback = function( table, ths ) {
            ths[4].classList.add( "borderleft" );
            ths[8].classList.add( "borderleft" );
            ths[12].classList.add( "borderleft" );
            let tr = rkWebUtil.elemaker( "tr", null );
            rkWebUtil.elemaker( "th", tr, { "text": "",
                                            "attributes": { "colspan": 4 },
                                            "classes": [ "borderleft" ] } );
            rkWebUtil.elemaker( "th", tr, { "text": "Last Detection",
                                            "attributes": { "colspan": 4 },
                                            "classes": [ "borderleft" ] } );
            rkWebUtil.elemaker( "th", tr, { "text": "Max Detection",
                                            "attributes": { "colspan": 4 },
                                            "classes": [ "borderleft" ] } );
            rkWebUtil.elemaker( "th", tr, { "text": "Last Forced",
                                            "attributes": { "colspan": 4 },
                                            "classes": [ "borderleft" ] } );
            table.prepend( tr );
        }
        this.fields = fields;
        this.data = data;
        this.objtable = new rkWebUtil.SortableTable(
            data,
            fields,
            rowrenderer,
            { 'dictoflists': true,
              'hdrs': hdrs,
              'colorclasses': [ 'whitebg', 'greybg' ],
              'colorlength' : 3,
              'headercallback': headercallback
            }
        );

        // TODO: info about search criteria
        this.topdiv.appendChild( this.objtable.table );

        let bdiv = rkWebUtil.elemaker( 'div', this.topdiv );
        rkWebUtil.button( bdiv, 'Download CSV', () => { self.download_csv(); } );
    }


    show_object_info( objid )
    {
        let self = this;
        let pv = this.context.procver_widget.value;

        rkWebUtil.wipeDiv( this.context.objectinfodiv );
        rkWebUtil.elemaker( "p", this.context.objectinfodiv,
                            { "text": "Loading object " + objid + " for processing version " + pv,
                              "classes": [ "warning", "bold", "italic" ] } );
        this.context.maintabs.selectTab( "objectinfo" );
        
        this.context.connector.sendHttpRequest( "/ltcv/getltcv/" + pv + "/" + objid, {},
                                                (data) => { self.actually_show_object_info( data ) } );
    }


    actually_show_object_info( data )
    {
        let info = new fastdbap.ObjectInfo( data, this.context, this.context.objectinfodiv );
        info.render_page();
    }

    download_csv()
    {
        let rows = [];
        rows.push( this.fields.join(',') );
        for ( let i in this.data[ this.fields[0] ] ) {
            let row = [];
            for ( let f of this.fields ) {
                row.push( this.data[f][i] );
            }
            rows.push( row.join(',') );
        }
        let blob = new Blob( [ rows.join('\n') ], { type: 'text/csv;charset=utf-8' } );
        let url = URL.createObjectURL( blob );
        let a = rkWebUtil.elemaker( 'a', document.body,
                                    { 'attributes': { 'href': url, 'download': 'object_list.csv' } } );
        a.click();
        document.body.removeChild( a );
        URL.revokeObjectURL( url );
    }

}

// **********************************************************************
// Make it into a module

export { }
