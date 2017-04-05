"use strict";

//window.onload = function(){alert("I AM MODULE FFS")}
//console.log("FFS CAN YOU WORK FUCKER")
/*jQuery( document ).ready(
  function(){
   alert("Jquery accessible")
  }
)*/

jQuery(document).ready(function(){
    $('[data-toggle="tooltip"]').tooltip(); 
});

/*ckan.module('csvmetadata-info-popup', function($){
  return {
    initialize: function () {
      console.log("I've been initialized for element: ", this.el);
      this.el.popover({title: "",
                       content: this.options.text,
                       placement: 'left'});
    }
  }
}) */
