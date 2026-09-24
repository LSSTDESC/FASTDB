import { fastdbap } from "./fastdb_ns.js"
import { rkWebUtil } from "./rkwebutil.js";

// **********************************************************************
// **********************************************************************
// **********************************************************************

fastdbap.ObjectSearch = class
{
    constructor( context, parentdiv )
    {
        this.context = context;
        this.topdiv = rkWebUtil.elemaker( "div", parentdiv, { "classes": [ "hbox", "minwcontent" ] } );
    }


    render_page()
    {
        let self = this;
        let table, tr, td, div, superdiv, subdiv,hbox, vbox, p, span;

        rkWebUtil.wipeDiv( this.topdiv );

        // search by diaobject id

        div = rkWebUtil.elemaker( "div", this.topdiv, { "classes": [ "searchinner", "xmarginright", "maxwcontent" ] } );
        p = rkWebUtil.elemaker( "p", div, { "text": "diaobjectid:" } );
        rkWebUtil.elemaker( "br", p );
        this.diaobjectid_widget = rkWebUtil.elemaker( "input", p, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "br", p );
        rkWebUtil.button( p, "Show", (e) => { self.show_object_info(); } );
        p = rkWebUtil.elemaker( "p", div );
        rkWebUtil.button( p, "Show Random Obj", (e) => { self.show_random_obj(); } );

        // More general search

        superdiv = rkWebUtil.elemaker( "div", this.topdiv, { "classes": [ "vbox" ] } );
        div = rkWebUtil.elemaker( "div", superdiv, { "classes": [ "maxwcontent", "hbox" ] } );
        subdiv = rkWebUtil.elemaker( "div", superdiv, { "classes": [ "maxwcontent", "hbox", "searchinner" ] } );
        rkWebUtil.button( subdiv, "Search", (e) => { self.object_search() } );
        rkWebUtil.elemaker( "span", subdiv, { "text": "Limit:", "classes": [ "mmarginleft" ] } );
        this.searchlimit = rkWebUtil.elemaker( "input", subdiv, { "classes": [ "mmarginright" ],
                                                                  "attributes": { "type": "number",
                                                                                  "min": 10,
                                                                                  "max": 1000,
                                                                                  "value": 100,
                                                                                  "sytle": "width: 4em" } } );
        this.searchlimit.addEventListener( "blur", (e) => {
            self.searchlimit.value = rkWebUtil.parseIntInRange( self.searchlimit.value, 10, 1000, 100 )
        } );
        rkWebUtil.elemaker( "text", subdiv, { "text": "Offset:" } );
        this.searchoffset = rkWebUtil.elemaker( "input", subdiv, { "classes": [ "mmarginright" ],
                                                                   "attributes": { "type": "number",
                                                                                   "min": 0,
                                                                                   "value": 0,
                                                                                   "style": "width: 6em" } } );
        rkWebUtil.elemaker( "text", subdiv, { "text": "Sort by:" } );
        this.searchsort = rkWebUtil.elemaker( "select", subdiv );
        let sorttext = { 'rootid': 'rootid',
                         'ra/dec': 'SPECIAL-ra/dec',
                         'dec/ra': 'SPECIAL-dec/ra',
                         'mjd_firstdetection': 'firstdet_mjd',
                         'mjd_lastdetection': 'lastdet_mjd',
                         'mjd_maxdetection': 'maxdet_mjd',
                         'mjd_lastforced': 'lastforced_mjd',
                         'flux_firstdetection': 'firstdet_flux',
                         'flux_lastdetection': 'lastdet_flux',
                         'flux_maxdetection': 'maxdet_flux',
                         'flux_lastforced': 'lastforced_flux',
                         'n detections': 'ndets',
                         'n forced phot points': 'nfrc',
                         'n dets mag≤24': 'ndets24',
                         'n dets mag≤23': 'ndets23',
                         'n dets mag≤22': 'ndets22',
                         'n dets mag≤21': 'ndets21',
                         'n s/n ≥10': 'nsn10',
                         'n s/n ≥7': 'nsn10',
                         'n s/n ≥5': 'nsn10'
                       }
        for ( let sortdisplay in sorttext ) {
            let sortkey = sorttext[ sortdisplay ];
            let wid = rkWebUtil.elemaker( "option", this.searchsort, { "text": sortdisplay,
                                                                       "attributes": { "value": sortkey } } );
            if ( sortkey == "rootid" ) wid.setAttribute( "selected", 1 );
        }
        this.sortorder = rkWebUtil.elemaker( "select", subdiv, { "classes": [ "xmarginleft" ] } );
        for ( let order of [ "ascending", "descending" ] ) {
            rkWebUtil.elemaker( "option", this.sortorder, { "text":  order, "attributes": { "value": order } } );
        }


        // search by ra/dec

        vbox = rkWebUtil.elemaker( "div", div, { "classes": [ "vbox", "xmarginright", "searchinner" ] } );
        table = rkWebUtil.elemaker( "table", vbox, { "classes": [ "borderless" ] } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "RA:", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.ra_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "text", td, { "text": "°" } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Dec:", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.dec_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "text", td, { "text": "°" } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "radius:", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.radius_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 10 } } );
        rkWebUtil.elemaker( "text", td, { "text": '"' } );

        // statbands

        vbox = rkWebUtil.elemaker( "div", div, { "classes": [ "vbox", "xmarginright", "searchinner" ] } );
        hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox", "bold" ] } );
        hbox.innerHTML = "Consider<br>bands:";
        hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox" ] } );
        this.u_checkbox = rkWebUtil.elemaker( "input", hbox, { "id": "u_statband_checkbox",
                                                               "attributes": { "type": "checkbox",
                                                                               "checked": 1 } } );
        rkWebUtil.elemaker( "label", hbox, { "text": "u ", "attributes": { "for": "u_statband_checkbox" } } );
        this.g_checkbox = rkWebUtil.elemaker( "input", hbox, { "id": "g_statband_checkbox",
                                                               "attributes": { "type": "checkbox",
                                                                               "checked": 1 } } );
        rkWebUtil.elemaker( "label", hbox, { "text": "g ", "attributes": { "for": "g_statband_checkbox" } } );
        hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox" ] } );
        this.r_checkbox = rkWebUtil.elemaker( "input", hbox, { "id": "r_statband_checkbox",
                                                               "attributes": { "type": "checkbox",
                                                                               "checked": 1 } } );
        rkWebUtil.elemaker( "label", hbox, { "text": "r ", "attributes": { "for": "r_statband_checkbox" } } );
        this.i_checkbox = rkWebUtil.elemaker( "input", hbox, { "id": "i_statband_checkbox",
                                                               "attributes": { "type": "checkbox",
                                                                               "checked": 1 } } );
        rkWebUtil.elemaker( "label", hbox, { "text": "i ", "attributes": { "for": "i_statband_checkbox" } } );
        hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox" ] } );
        this.z_checkbox = rkWebUtil.elemaker( "input", hbox, { "id": "z_statband_checkbox",
                                                               "attributes": { "type": "checkbox",
                                                                               "checked": 1 } } );
        rkWebUtil.elemaker( "label", hbox, { "text": "z ", "attributes": { "for": "z_statband_checkbox" } } );
        this.y_checkbox = rkWebUtil.elemaker( "input", hbox, { "id": "y_statband_checkbox",
                                                               "attributes": { "type": "checkbox",
                                                                               "checked": 1 } } );
        rkWebUtil.elemaker( "label", hbox, { "text": "Y ", "attributes": { "for": "y_statband_checkbox" } } );


        // search by first/max/last mjd/mag

        vbox = rkWebUtil.elemaker( "div", div, { "classes": [ "vbox", "xmarginright", "searchinner" ] } );
        table = rkWebUtil.elemaker( "table", vbox, { "classes": [ "borderless"] } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "th", tr );
        td = rkWebUtil.elemaker( "th", tr, { "text": "min" } );
        td = rkWebUtil.elemaker( "th", tr, { "text": "max" } );
        td = rkWebUtil.elemaker( "th", tr );
        td = rkWebUtil.elemaker( "th", tr, { "text": "min" } );
        td = rkWebUtil.elemaker( "th", tr, { "text": "max" } );

        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "First detection mjd", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.firstdetminmjd_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 6 } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.firstdetmaxmjd_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 6 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "mag" } );
        td = rkWebUtil.elemaker( "td", tr );
        this.firstdetminmag_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.firstdetmaxmag_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );

        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Last detection mjd", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.lastdetminmjd_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 6 } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.lastdetmaxmjd_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 6 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "mag" } );
        td = rkWebUtil.elemaker( "td", tr );
        this.lastdetminmag_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.lastdetmaxmag_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );

        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Max detection mjd", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.maxdetminmjd_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 6 } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.maxdetmaxmjd_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 6 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "mag" } );
        td = rkWebUtil.elemaker( "td", tr );
        this.maxdetminmag_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.maxdetmaxmag_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );

        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Min n. detections:", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr );
        this.minnumdet_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 6 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "Latest mag", "classes": [ "right" ],
                                             "attributes": { "colspan": 2 } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.minlastforcedmag_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr );
        this.maxlastforcedmag_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );

        // Detection counts

        vbox = rkWebUtil.elemaker( "div", div, { "classes": [ "vbox", "xmarginright", "searchinner" ] } );
        hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox", "bold" ],
                                                  "text": "Number of Detections with:" } );
        hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox" ] } );
        table = rkWebUtil.elemaker( "table", hbox, { "classes": [ "borderless", "mmarginright" ] } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "mag ≤ 24", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.mindetmaglt24_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxdetmaglt24_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "mag ≤ 23", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.mindetmaglt23_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxdetmaglt23_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "mag ≤ 22", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.mindetmaglt22_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxdetmaglt22_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "mag ≤ 21", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.mindetmaglt21_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxdetmaglt21_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );

        table = rkWebUtil.elemaker( "table", hbox, { "classes": [ "borderless" ] } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "s/n ≥ 5", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.mindetsngt5_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxdetsngt5_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "s/n ≥ 7", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } )
        this.mindetsngt7_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } )
        this.maxdetsngt7_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "s/n ≥ 10", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } )
        this.mindetsngt10_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } )
        this.maxdetsngt10_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );

        table = rkWebUtil.elemaker( "table", vbox );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "reliability > 0.5", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.minreliabilitygtp5_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxreliabilitygtp5_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "reliability > 0.8", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.minreliabilitygtp8_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxreliabilitygtp8_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );

        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "reliability > 0.6", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.minreliabilitygtp6_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxreliabilitygtp6_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "reliability > 0.9", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.minreliabilitygtp9_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxreliabilitygtp9_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );

        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr, { "text": "reliability > 0.7", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.minreliabilitygtp7_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxreliabilitygtp7_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "reliability > 0.95", "classes": [ "right" ] } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≥" } );
        this.minreliabilitygtp95_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );
        td = rkWebUtil.elemaker( "td", tr, { "text": "≤" } );
        this.maxreliabilitygtp95_widget = rkWebUtil.elemaker( "input", td, { "attributes": { "size": 4 } } );

        
        // Window... not currently supported by ltcv.py::object_search

        // vbox = rkWebUtil.elemaker( "div", div, { "classes": [ "vbox", "xmarginright", "searchinner" ] } );
        // rkWebUtil.elemaker( "p", vbox, { "text": "Search window:", "classes": [ "bold" ] } );
        // hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox" ], "text": "MJD: " } );
        // this.window_mjd0_widget = rkWebUtil.elemaker( "input", hbox, { "attributes": { "size": 6 } } );
        // rkWebUtil.elemaker( "text", hbox, { "text": " to " } );
        // this.window_mjd1_widget = rkWebUtil.elemaker( "input", hbox, { "attributes": { "size": 6 } } );
        // hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox", "xmargintop" ], "text": "Min detections: " } );
        // this.dets_in_window_widget = rkWebUtil.elemaker( "input", hbox, { "attributes": { "size": 3 } } );
    }


    object_search()
    {
        let self = this;

        let procver = this.context.procver_widget.value;
        if ( procver == "—select one —" ) {
            alert( "Select a processing version to search" );
            return;
        }

        let searchcriteria = {};
        if ( this.ra_widget.value.trim().length > 0 )
            searchcriteria.ra = this.ra_widget.value.trim();
        if ( this.dec_widget.value.trim().length > 0 )
            searchcriteria.dec = this.dec_widget.value.trim();
        if ( this.radius_widget.value.trim().length > 0 )
            searchcriteria.radius = this.radius_widget.value.trim();

        let statbands = [];
        if ( this.u_checkbox.checked ) statbands.push( 'u' );
        if ( this.g_checkbox.checked ) statbands.push( 'g' );
        if ( this.r_checkbox.checked ) statbands.push( 'r' );
        if ( this.i_checkbox.checked ) statbands.push( 'i' );
        if ( this.z_checkbox.checked ) statbands.push( 'z' );
        if ( this.y_checkbox.checked ) statbands.push( 'Y' );
        // If all are checked, don't include it as a criterion
        if ( statbands.length < 6 )
            searchcriteria.searchband = statbands;

        if ( this.firstdetminmjd_widget.value.trim().length > 0 )
            searchcriteria.firstdet_mjd_min = this.firstdetminmjd_widget.value.trim();
        if ( this.firstdetmaxmjd_widget.value.trim().length > 0 )
            searchcriteria.firstdet_mjd_max = this.firstdetmaxmjd_widget.value.trim();
        if ( this.firstdetminmag_widget.value.trim().length > 0 )
            searchcriteria.firstdet_flux_max = (
                10 ** ( ( 31.4 - parseFloat(this.firstdetminmag_widget.value.trim()) ) / 2.5 ) );
        if ( this.firstdetmaxmag_widget.value.trim().length > 0 )
            searchcriteria.firstdet_flux_min = (
                10 ** ( ( 31.4 - parseFloat(this.firstdetmaxmag_widget.value.trim()) ) / 2.5 ) );

        if ( this.lastdetminmjd_widget.value.trim().length > 0 )
            searchcriteria.lastdet_mjd_min = this.lastdetminmjd_widget.value.trim();
        if ( this.lastdetmaxmjd_widget.value.trim().length > 0 )
            searchcriteria.lastdet_mjd_max = this.lastdetmaxmjd_widget.value.trim();
        if ( this.lastdetminmag_widget.value.trim().length > 0 )
            searchcriteria.lastdet_flux_max = (
                10 ** ( ( 31.4 - parseFloat(this.lastdetminmag_widget.value.trim()) ) / 2.5 ) );
        if ( this.lastdetmaxmag_widget.value.trim().length > 0 )
             searchcriteria.lastdet_flux_min = (
                10 ** ( ( 31.4 - parseFloat(this.lastdetmaxmag_widget.value.trim()) ) / 2.5 ) );

        if ( this.maxdetminmjd_widget.value.trim().length > 0 )
            searchcriteria.maxdet_mjd_min = this.maxdetminmjd_widget.value.trim();
        if ( this.maxdetmaxmjd_widget.value.trim().length > 0 )
            searchcriteria.maxdet_mjd_max = this.maxdetmaxmjd_widget.value.trim();
        if ( this.maxdetminmag_widget.value.trim().length > 0 )
            searchcriteria.maxdet_flux_max = (
                10 ** ( ( 31.4 - parseFloat(this.maxdetminmag_widget.value.trim()) ) / 2.5 ) );
        if ( this.maxdetmaxmag_widget.value.trim().length > 0 )
            searchcriteria.maxdet_flux_min = (
                10 ** ( ( 31.4 - parseFloat(this.maxdetmaxmag_widget.value.trim()) ) / 2.5 ) );

        if ( this.minnumdet_widget.value.trim().length > 0 )
            searchcriteria.ndets_min = this.minnumdet_widget.value.trim();
        if ( this.minlastforcedmag_widget.value.trim().length > 0 )
            searchcriteria.lastforced_flux_max = (
                10 ** ( ( 31.4 - parseFloat(this.minlastforcedmag_widget.value.trim()) ) / 2.5 ) );
        if ( this.maxlastforcedmag_widget.value.trim().length > 0 )
            searchcriteria.lastforced_flux_min = (
                10 ** ( ( 31.4 - parseFloat(this.maxlastforcedmag_widget.value.trim()) ) / 2.5 ) );

        for ( let maglim of [ 21, 22, 23, 24 ] ) {
            let val = this['mindetmaglt' + maglim + "_widget"].value.trim();
            if ( val.length > 0 ) searchcriteria['ndets' + maglim + '_min'] = val;
            val = this['maxdetmaglt' + maglim + "_widget"].value.trim()
            if ( val.length > 0 ) searchcriteria['ndets' + maglim + '_max'] = val;
        }
        for ( let snlim of [ 5, 7, 10 ] ) {
            let val = this['mindetsngt' + snlim + '_widget'].value.trim();
            if ( val.length > 0 ) searchcriteria['nsn' + snlim + '_min'] = val;
            val = this['maxdetsngt' + snlim + '_widget'].value.trim();
            if ( val.length > 0 ) searchcriteria['nsn' + snlim + '_max'] = val;
        }

        // ltcv.object_search doesn't have a search window
        // if ( this.window_mjd0_widget.value.trim().length > 0 )
        //     searchcriteria.window_t0 = this.window_mjd0_widget.value.trim();
        // if ( this.window_mjd1_widget.value.trim().length > 0 )
        //     searchcriteria.window_t1 = this.window_mjd1_widget.value.trim();
        // if ( this.dets_in_window_widget.value.trim().length > 0 )
        //     searchcriteria.min_window_numdetections = this.dets_in_window_widget.value.trim();

        // Sort, Limit, Offset

        let descending = false
        if ( this.sortorder.value == 'descending' ) descending = true;

        let sortby = this.searchsort.value;
        if ( sortby == 'SPECIAL-ra/dec' ) {
            if ( descending )
                sortby = [ '-ra', '-dec' ];
            else
                sortby = [ 'ra', 'dec' ];
        }
        else if ( sortby == 'SPECIAL-/dec/ra' ) {
            if ( descending )
                sortby = [ '-dec', '-ra' ];
            else
                sortby = [ 'dec', 'ra' ];
        }
        if ( sortby != null ) {
            if ( descending )
                searchcriteria['orderby'] = "-" + sortby;
            else
                searchcriteria['orderby'] = sortby;
        }

        searchcriteria['limit'] = this.searchlimit.value;
        searchcriteria['offset'] = this.searchoffset.value;

        // Do

        rkWebUtil.wipeDiv( this.context.objectlistdiv );
        this.context.maintabs.selectTab( "objectlist" );
        rkWebUtil.elemaker( "p", this.context.objectlistdiv, { "text": "Searching for objects...",
                                                                "classes": [ "bold", "italic", "warning" ] } );
        this.context.connector.sendHttpRequest( "objectsearch/" + procver, searchcriteria,
                                               (data) => { self.context.object_search_results(data); } );
    }


    show_object_info()
    {
        let self = this;
        let objid = this.diaobjectid_widget.value;
        let pv = this.context.procver_widget.value;

        rkWebUtil.wipeDiv( this.context.objectinfodiv );
        rkWebUtil.elemaker( "p", this.context.objectinfodiv,
                            { "text": "Loading object " + objid + " for processing version " + pv,
                              "classes": [ "warning", "bold", "italic" ] } );
        this.context.maintabs.selectTab( "objectinfo" );

        this.context.connector.sendHttpRequest( "ltcv/getltcv/" + pv + "/" + objid,
                                                { 'return_object_info': 1 },
                                                (data) => { self.actually_show_object_info( data ) } );
    }

    show_random_obj()
    {
        let self = this;
        let pv = this.context.procver_widget.value;

        rkWebUtil.wipeDiv( this.context.objectinfodiv );
        rkWebUtil.elemaker( "p", this.context.objectinfodiv,
                            { "text": "Loading random object for processing version " + pv,
                              "classes": [ "warning", "bold", "italic" ] } );
        this.context.maintabs.selectTab( "objectinfo" )

        this.context.connector.sendHttpRequest( "ltcv/getrandomltcv/" + pv,
                                                { 'return_object_info': 1 },
                                                (data) => { self.actually_show_object_info( data ) } );
    }

    actually_show_object_info( data )
    {
        let info = new fastdbap.ObjectInfo( data, this.context, this.context.objectinfodiv );
        info.render_page();
    }

}

// **********************************************************************
// Make it into a module

export { }
