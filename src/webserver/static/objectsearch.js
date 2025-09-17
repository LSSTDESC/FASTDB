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
        let table, tr, td, div, hbox, vbox, p;

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

        // search by ra/dec

        div = rkWebUtil.elemaker( "div", this.topdiv, { "classes": [ "maxwcontent", "hbox" ] } );
        vbox = rkWebUtil.elemaker( "div", div, { "classes": [ "vbox", "xmarginright", "searchinner" ] } );
        table = rkWebUtil.elemaker( "table", vbox, { "classes": [ "borderless" ] } );
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr );
        rkWebUtil.button( td, "Search", (e) => { self.object_search() } );
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

        // Window

        vbox = rkWebUtil.elemaker( "div", div, { "classes": [ "vbox", "xmarginright", "searchinner" ] } );
        rkWebUtil.elemaker( "p", vbox, { "text": "Search window:", "classes": [ "bold" ] } );
        hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox" ], "text": "MJD: " } );
        this.window_mjd0_widget = rkWebUtil.elemaker( "input", hbox, { "attributes": { "size": 6 } } );
        rkWebUtil.elemaker( "text", hbox, { "text": " to " } );
        this.window_mjd1_widget = rkWebUtil.elemaker( "input", hbox, { "attributes": { "size": 6 } } );
        hbox = rkWebUtil.elemaker( "div", vbox, { "classes": [ "hbox", "xmargintop" ], "text": "Min detections: " } );
        this.dets_in_window_widget = rkWebUtil.elemaker( "input", hbox, { "attributes": { "size": 3 } } );
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
            searchcriteria.statbands = statbands;
        
        if ( this.firstdetminmjd_widget.value.trim().length > 0 )
            searchcriteria.mint_firstdetection = this.firstdetminmjd_widget.value.trim();
        if ( this.firstdetmaxmjd_widget.value.trim().length > 0 )
            searchcriteria.maxt_firstdetection = this.firstdetmaxmjd_widget.value.trim();
        if ( this.firstdetminmag_widget.value.trim().length > 0 )
            searchcriteria.minmag_firstdetection = this.firstdetminmag_widget.value.trim();
        if ( this.firstdetmaxmag_widget.value.trim().length > 0 )
            searchcriteria.maxmag_firstdetection = this.firstdetmaxmag_widget.value.trim();
            
        if ( this.lastdetminmjd_widget.value.trim().length > 0 )
            searchcriteria.mint_lastdetection = this.lastdetminmjd_widget.value.trim();
        if ( this.lastdetmaxmjd_widget.value.trim().length > 0 )
            searchcriteria.maxt_lastdetection = this.lastdetmaxmjd_widget.value.trim();
        if ( this.lastdetminmag_widget.value.trim().length > 0 )
            searchcriteria.minmag_lastdetection = this.lastdetminmag_widget.value.trim();
        if ( this.lastdetmaxmag_widget.value.trim().length > 0 )
            searchcriteria.maxmag_lastdetection = this.lastdetmaxmag_widget.value.trim();
            
        if ( this.maxdetminmjd_widget.value.trim().length > 0 )
            searchcriteria.mint_maxdetection = this.maxdetminmjd_widget.value.trim();
        if ( this.maxdetmaxmjd_widget.value.trim().length > 0 )
            searchcriteria.maxt_maxdetection = this.maxdetmaxmjd_widget.value.trim();
        if ( this.maxdetminmag_widget.value.trim().length > 0 )
            searchcriteria.minmag_maxdetection = this.maxdetminmag_widget.value.trim();
        if ( this.maxdetmaxmag_widget.value.trim().length > 0 )
            searchcriteria.maxmag_maxdetection = this.maxdetmaxmag_widget.value.trim();
        
        if ( this.minnumdet_widget.value.trim().length > 0 )
            searchcriteria.min_numdetections = this.minnumdet_widget.value.trim();
        if ( this.minlastforcedmag_widget.value.trim().length > 0 )
            searchcriteria.min_lastmag = this.minlastforcedmag_widget.value.trim();
        if ( this.maxlastforcedmag_widget.value.trim().length > 0 )
            searchcriteria.max_lastmag = this.maxlastforcedmag_widget.value.trim();

        if ( this.window_mjd0_widget.value.trim().length > 0 )
            searchcriteria.window_t0 = this.window_mjd0_widget.value.trim();
        if ( this.window_mjd1_widget.value.trim().length > 0 )
            searchcriteria.window_t1 = this.window_mjd1_widget.value.trim();
        if ( this.dets_in_window_widget.value.trim().length > 0 )
            searchcriteria.min_window_numdetections = this.dets_in_window_widget.value.trim();
        
        
        rkWebUtil.wipeDiv( this.context.objectlistdiv );
        this.context.maintabs.selectTab( "objectlist" );
        rkWebUtil.elemaker( "p", this.context.objectlistdiv, { "text": "Searching for objects...",
                                                                "classes": [ "bold", "italic", "warning" ] } );
        this.context.connector.sendHttpRequest( "/objectsearch/" + procver, searchcriteria,
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

        this.context.connector.sendHttpRequest( "/ltcv/getltcv/" + pv + "/" + objid, {},
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

        this.context.connector.sendHttpRequest( "/ltcv/getrandomltcv/" + pv, {},
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
