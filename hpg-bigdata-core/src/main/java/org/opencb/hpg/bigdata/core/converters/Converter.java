package org.opencb.hpg.bigdata.core.converters;

/**
 * Converts between two Java classes.
 * 
 * @author Joaquín Tárraga Giménez &lt;joaquintarraga@gmail.com&gt;
 */
public interface Converter<S, T> {
	    
	    public T forward(S obj);
	    
	    public S backward(T obj);

}
