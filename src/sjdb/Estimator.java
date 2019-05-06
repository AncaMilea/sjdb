package sjdb;

import com.sun.prism.shader.AlphaTexture_Color_AlphaTest_Loader;
import org.w3c.dom.Attr;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/*
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());

		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}

		op.setOutput(output);
	}

	public void visit(Project op) {
		Relation output = new Relation(op.getInput().getOutput().getTupleCount());
		Relation input = op.getInput().getOutput();

		Attribute now;
		Iterator<Attribute> it = input.getAttributes().iterator();
		while (it.hasNext()) {
			now = it.next();
			if(op.getAttributes().contains(now))
				output.addAttribute(new Attribute(now));
		}

		op.setOutput(output);
	}

	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		Relation output;

		if(op.getPredicate().equalsValue()) {
			output = new Relation(input.getTupleCount()/input.getAttribute(op.getPredicate().getLeftAttribute()).getValueCount());
			Attribute left_pred= op.getPredicate().getLeftAttribute();
			for(Attribute it:input.getAttributes()) {
				if(it.equals(left_pred)){
					output.addAttribute(new Attribute(it.getName(),1));
				}else {
					output.addAttribute(new Attribute(it));
				}
			}
		}else{
			Attribute left = input.getAttribute(op.getPredicate().getLeftAttribute());
			Attribute right = input.getAttribute(op.getPredicate().getRightAttribute());

			output = new Relation(input.getTupleCount()/Math.max(left.getValueCount(),right.getValueCount()));

			for (Attribute it:input.getAttributes()) {
				if (it.equals(left) || it.equals(right)) {
					output.addAttribute(new Attribute(it.getName(), Math.min(left.getValueCount(), right.getValueCount())));
				} else {
					output.addAttribute(new Attribute(it));
				}
			}
		}
		op.setOutput(output);

	}

	public void visit(Product op) {
		Relation left = op.getLeft().getOutput();
		Relation right = op.getRight().getOutput();
		Relation output = new Relation(left.getTupleCount()*right.getTupleCount());

		for (Attribute iter_l: left.getAttributes()) {
			output.addAttribute(new Attribute(iter_l));
		}

		for (Attribute iter_r: right.getAttributes()) {
			output.addAttribute(new Attribute(iter_r));
		}

		op.setOutput(output);
	}

	public void visit(Join op) {
		Relation left = op.getLeft().getOutput();
		Relation right = op.getRight().getOutput();

		Attribute left_pred = op.getPredicate().getLeftAttribute();
		Attribute right_pred = op.getPredicate().getRightAttribute();

		Relation output = new Relation(left.getTupleCount() * right.getTupleCount() /Math.max(left.getAttribute(left_pred).getValueCount(),right.getAttribute(right_pred).getValueCount()));

		for(Attribute left_it: left.getAttributes()){
			if(left_it.equals(left_pred)){
				output.addAttribute(new Attribute(left_it.getName(),Math.min(left_pred.getValueCount(),right_pred.getValueCount())));
			}else {
				output.addAttribute(new Attribute(left_it.getName(),left_it.getValueCount()));
			}
		}

		for (Attribute right_it:right.getAttributes()){
			if(right_it.equals(right_pred)) {
				output.addAttribute(new Attribute(right_it.getName(), Math.min(left_pred.getValueCount(), right_pred.getValueCount())));
			}
			else {
				output.addAttribute(new Attribute(right_it.getName(),right_it.getValueCount()));
			}
		}

		op.setOutput(output);


	}
}
