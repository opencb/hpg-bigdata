package org.opencb.hpgbigdata.core.converters;

/**
 * Converts between two Java classes.
 * 
 * @author Joaquín Tárraga Giménez &lt;joaquintarraga@gmail.com&gt;
 */
public interface Converter<a, b> {
	    
	    public b forward(a obj);
	    
	    public a backward(b obj);
}
